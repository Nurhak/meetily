"use client"

import { useCallback, useEffect, useRef, useState } from "react"
import { invoke } from "@tauri-apps/api/core"
import { listen } from "@tauri-apps/api/event"
import { toast } from "sonner"
import { Smartphone, QrCode, Trash2, RotateCcw, AlertCircle, Loader2, Wifi } from "lucide-react"
import { Switch } from "./ui/switch"
import { Button } from "./ui/button"
import { Input } from "./ui/input"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "./ui/dialog"

// Mirrors sync-bridge's SyncStatus / PairingView / DeviceView / DeletedMeetingView (camelCase).
interface SyncStatus {
  enabled: boolean
  running: boolean
  name: string
  hubId: string | null
  fingerprint: string | null
  port: number | null
  addresses: string[]
  pairedDevices: number
  error: string | null
}

interface PairingView {
  code: string
  verification: string
  qrUri: string
  qrSvg: string
  expiresInSecs: number
  addresses: string[]
}

interface DeviceView {
  deviceId: string
  name: string
  platform: string
  appVersion: string | null
  scope: string
  pairedAt: string
  lastSeenAt: string | null
}

interface DeletedMeetingView {
  meetingId: string
  title: string
  deletedAt: string
  recordedOn: string | null
}

const errorText = (e: unknown) => (typeof e === "string" ? e : e instanceof Error ? e.message : String(e))

const formatDate = (iso: string | null) => {
  if (!iso) return null
  const d = new Date(iso)
  return isNaN(d.getTime()) ? null : d.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" })
}

const platformLabel = (platform: string) =>
  ({ ios: "iPhone / iPad", android: "Android", macos: "Mac", windows: "Windows" } as Record<string, string>)[platform] ?? platform

export function PhoneSyncSettings() {
  const [status, setStatus] = useState<SyncStatus | null>(null)
  const [busy, setBusy] = useState(false)
  const [name, setName] = useState("")
  const [devices, setDevices] = useState<DeviceView[]>([])
  const [deleted, setDeleted] = useState<DeletedMeetingView[]>([])
  const [pairing, setPairing] = useState<PairingView | null>(null)
  const [pairingOpen, setPairingOpen] = useState(false)
  const [secondsLeft, setSecondsLeft] = useState(0)
  const [confirmRemove, setConfirmRemove] = useState<string | null>(null)
  const pairingOpenRef = useRef(false)

  const loadStatus = useCallback(async () => {
    try {
      const s = await invoke<SyncStatus>("sync_status")
      setStatus(s)
      setName(s.name)
      return s
    } catch {
      // The service starts once the database is ready; try again shortly.
      return null
    }
  }, [])

  const loadLists = useCallback(async () => {
    try {
      const [d, r] = await Promise.all([
        invoke<DeviceView[]>("sync_devices"),
        invoke<DeletedMeetingView[]>("sync_recently_deleted"),
      ])
      setDevices(d)
      setDeleted(r)
    } catch {
      setDevices([])
      setDeleted([])
    }
  }, [])

  // Status (retrying while the service starts), then the lists when sync is running.
  useEffect(() => {
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const poll = async () => {
      const s = await loadStatus()
      if (cancelled) return
      if (!s) {
        timer = setTimeout(poll, 1000)
      } else if (s.running) {
        loadLists()
      }
    }
    poll()
    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
    }
  }, [loadStatus, loadLists])

  // Phones pairing, and meetings deleted or restored elsewhere.
  useEffect(() => {
    const unlisten: Array<Promise<() => void>> = [
      listen<{ deviceId: string }>("sync-device-paired", async () => {
        if (pairingOpenRef.current) {
          pairingOpenRef.current = false
          setPairingOpen(false)
          setPairing(null)
        }
        toast.success("Phone paired", { description: "Meetings will now sync with this computer." })
        await loadStatus()
        await loadLists()
      }),
      listen("sync-meetings-changed", () => {
        loadLists()
      }),
    ]
    return () => {
      unlisten.forEach(p => p.then(f => f()))
    }
  }, [loadStatus, loadLists])

  // Pairing code countdown.
  useEffect(() => {
    if (!pairingOpen || !pairing) return
    const expiresAt = Date.now() + pairing.expiresInSecs * 1000
    setSecondsLeft(pairing.expiresInSecs)
    const timer = setInterval(() => {
      setSecondsLeft(Math.max(0, Math.round((expiresAt - Date.now()) / 1000)))
    }, 1000)
    return () => clearInterval(timer)
  }, [pairingOpen, pairing])

  const setEnabled = async (enabled: boolean) => {
    setBusy(true)
    try {
      const s = await invoke<SyncStatus>("sync_set_enabled", { enabled })
      setStatus(s)
      if (s.running) {
        await loadLists()
      } else {
        setDevices([])
        setDeleted([])
      }
      if (enabled && s.error) {
        toast.error("Phone sync didn't start", { description: s.error })
      }
    } catch (e) {
      toast.error("Couldn't change phone sync", { description: errorText(e) })
    } finally {
      setBusy(false)
    }
  }

  const saveName = async () => {
    if (!status || name.trim() === status.name) return
    setBusy(true)
    try {
      const s = await invoke<SyncStatus>("sync_set_name", { name })
      setStatus(s)
      setName(s.name)
      toast.success("Name saved")
    } catch (e) {
      toast.error("Couldn't save the name", { description: errorText(e) })
    } finally {
      setBusy(false)
    }
  }

  const startPairing = async () => {
    try {
      const p = await invoke<PairingView>("sync_start_pairing")
      setPairing(p)
      pairingOpenRef.current = true
      setPairingOpen(true)
    } catch (e) {
      toast.error("Couldn't start pairing", { description: errorText(e) })
    }
  }

  const closePairing = (open: boolean) => {
    if (open) return
    pairingOpenRef.current = false
    setPairingOpen(false)
    setPairing(null)
    invoke("sync_cancel_pairing").catch(() => {})
  }

  const removeDevice = async (device: DeviceView) => {
    if (confirmRemove !== device.deviceId) {
      setConfirmRemove(device.deviceId)
      return
    }
    setConfirmRemove(null)
    try {
      await invoke("sync_remove_device", { deviceId: device.deviceId })
      toast.success(`${device.name} removed`, { description: "It has to pair again to sync." })
      await loadStatus()
      await loadLists()
    } catch (e) {
      toast.error("Couldn't remove the phone", { description: errorText(e) })
    }
  }

  const restore = async (meeting: DeletedMeetingView) => {
    try {
      await invoke("sync_restore_meeting", { meetingId: meeting.meetingId })
      toast.success("Meeting restored", { description: "It's back here and on your phones." })
      await loadLists()
    } catch (e) {
      toast.error("Couldn't restore the meeting", { description: errorText(e) })
    }
  }

  if (!status) {
    return (
      <div className="flex items-center gap-2 p-6 text-sm text-gray-600">
        <Loader2 className="h-4 w-4 animate-spin" />
        Phone sync is getting ready…
      </div>
    )
  }

  const expired = pairing !== null && secondsLeft === 0

  return (
    <div className="space-y-6">
      {/* On/off */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 shadow-sm">
        <div className="flex items-center justify-between">
          <div className="flex-1">
            <div className="flex items-center gap-2 mb-2">
              <Smartphone className="h-5 w-5 text-gray-600" />
              <h3 className="text-lg font-semibold text-gray-900">Phone sync</h3>
            </div>
            <p className="text-sm text-gray-600">
              Pair phones with the Sales Copilot app. Meetings, transcripts and recordings sync between them and
              this computer over your local network; nothing goes through the cloud. Meetings recorded on a phone
              are transcribed here with your transcription model.
            </p>
          </div>
          <div className="ml-6">
            <Switch checked={status.enabled} disabled={busy} onCheckedChange={setEnabled} />
          </div>
        </div>

        {status.enabled && status.error && (
          <div className="mt-4 flex items-start gap-3 p-3 bg-red-50 border border-red-200 rounded-md">
            <AlertCircle className="h-4 w-4 text-red-600 flex-shrink-0 mt-0.5" />
            <p className="text-sm text-red-800">Phone sync isn&apos;t running: {status.error}</p>
          </div>
        )}

        {status.running && (
          <div className="mt-4 flex items-start gap-2 text-sm text-gray-600">
            <Wifi className="h-4 w-4 flex-shrink-0 mt-0.5 text-green-600" />
            <span>
              Running on {status.addresses.length > 0 ? status.addresses.join(", ") : `port ${status.port}`}.
              Phones need to be on the same network.
            </span>
          </div>
        )}
      </div>

      {/* Name */}
      <div className="bg-white rounded-lg border border-gray-200 p-6 shadow-sm">
        <h3 className="text-base font-semibold text-gray-900 mb-1">Computer name</h3>
        <p className="text-sm text-gray-600 mb-3">What your phones call this computer.</p>
        <div className="flex gap-2">
          <Input
            value={name}
            maxLength={63}
            onChange={e => setName(e.target.value)}
            onKeyDown={e => e.key === "Enter" && saveName()}
          />
          <Button
            variant="outline"
            disabled={busy || name.trim() === "" || name.trim() === status.name}
            onClick={saveName}
          >
            Save
          </Button>
        </div>
      </div>

      {status.running && (
        <>
          {/* Phones */}
          <div className="bg-white rounded-lg border border-gray-200 p-6 shadow-sm">
            <div className="flex items-center justify-between mb-4">
              <div>
                <h3 className="text-base font-semibold text-gray-900">Phones</h3>
                <p className="text-sm text-gray-600">Phones paired with this computer.</p>
              </div>
              <Button variant="blue" onClick={startPairing}>
                <QrCode />
                Pair a phone
              </Button>
            </div>

            {devices.length === 0 ? (
              <p className="text-sm text-gray-500">No phones yet. Choose “Pair a phone” and scan the code with the app.</p>
            ) : (
              <ul className="divide-y divide-gray-100">
                {devices.map(device => (
                  <li key={device.deviceId} className="flex items-center justify-between py-3">
                    <div>
                      <p className="text-sm font-medium text-gray-900">{device.name}</p>
                      <p className="text-xs text-gray-500">
                        {platformLabel(device.platform)}
                        {device.appVersion ? ` · app ${device.appVersion}` : ""}
                        {formatDate(device.lastSeenAt)
                          ? ` · last synced ${formatDate(device.lastSeenAt)}`
                          : ` · paired ${formatDate(device.pairedAt) ?? ""}`}
                      </p>
                    </div>
                    <Button
                      variant={confirmRemove === device.deviceId ? "red" : "ghost"}
                      size="sm"
                      onClick={() => removeDevice(device)}
                      onBlur={() => setConfirmRemove(null)}
                    >
                      <Trash2 />
                      {confirmRemove === device.deviceId ? "Remove?" : "Remove"}
                    </Button>
                  </li>
                ))}
              </ul>
            )}

            {status.fingerprint && (
              <p className="mt-4 text-xs text-gray-400 break-all">
                Key fingerprint: <span className="font-mono">{status.fingerprint}</span>
              </p>
            )}
          </div>

          {/* Recently deleted */}
          <div className="bg-white rounded-lg border border-gray-200 p-6 shadow-sm">
            <h3 className="text-base font-semibold text-gray-900 mb-1">Recently deleted</h3>
            <p className="text-sm text-gray-600 mb-3">
              Deleting a meeting here or on a phone deletes it everywhere. Restore it within 30 days to bring it back.
            </p>
            {deleted.length === 0 ? (
              <p className="text-sm text-gray-500">Nothing deleted.</p>
            ) : (
              <ul className="divide-y divide-gray-100">
                {deleted.map(meeting => (
                  <li key={meeting.meetingId} className="flex items-center justify-between py-3">
                    <div>
                      <p className="text-sm font-medium text-gray-900">{meeting.title}</p>
                      <p className="text-xs text-gray-500">
                        Deleted {formatDate(meeting.deletedAt) ?? ""}
                        {meeting.recordedOn ? ` · recorded on ${meeting.recordedOn}` : ""}
                      </p>
                    </div>
                    <Button variant="outline" size="sm" onClick={() => restore(meeting)}>
                      <RotateCcw />
                      Restore
                    </Button>
                  </li>
                ))}
              </ul>
            )}
          </div>
        </>
      )}

      {/* Pairing */}
      <Dialog open={pairingOpen} onOpenChange={closePairing}>
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Pair a phone</DialogTitle>
            <DialogDescription>
              In the Sales Copilot app on your phone, choose “Pair with computer” and scan this code.
            </DialogDescription>
          </DialogHeader>
          {pairing && (
            <div className="flex flex-col items-center gap-4">
              <div
                className={`w-60 h-60 [&_svg]:w-full [&_svg]:h-full ${expired ? "opacity-20" : ""}`}
                // Generated by sync-bridge from the pairing URI (qrcode crate), not user content.
                dangerouslySetInnerHTML={{ __html: pairing.qrSvg }}
              />
              {expired ? (
                <Button variant="blue" onClick={startPairing}>
                  New code
                </Button>
              ) : (
                <>
                  <div className="text-center">
                    <p className="text-xs text-gray-500">Can&apos;t scan? Enter this code in the app</p>
                    <p className="font-mono text-2xl tracking-widest text-gray-900">{pairing.code}</p>
                  </div>
                  <div className="text-center">
                    <p className="text-xs text-gray-500">The phone shows this number when pairing by code; check they match</p>
                    <p className="font-mono text-lg text-gray-900">{pairing.verification}</p>
                  </div>
                  <p className="text-xs text-gray-400">
                    Expires in {Math.floor(secondsLeft / 60)}:{String(secondsLeft % 60).padStart(2, "0")}
                  </p>
                </>
              )}
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}

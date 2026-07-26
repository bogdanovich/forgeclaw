import { launcherFetch } from "@/api/http"

// API client for MintClaw Channel configuration.

interface MintClawInfoResponse {
  ws_url: string
  enabled: boolean
  configured?: boolean
}

interface MintClawSetupResponse {
  ws_url: string
  enabled: boolean
  configured?: boolean
  changed: boolean
}

const BASE_URL = ""

async function request<T>(path: string, options?: RequestInit): Promise<T> {
  const res = await launcherFetch(`${BASE_URL}${path}`, options)
  if (!res.ok) {
    throw new Error(`API error: ${res.status} ${res.statusText}`)
  }
  return res.json() as Promise<T>
}

export async function getMintClawInfo(): Promise<MintClawInfoResponse> {
  return request<MintClawInfoResponse>("/api/mintclaw/info")
}

export async function regenMintClawToken(): Promise<MintClawInfoResponse> {
  return request<MintClawInfoResponse>("/api/mintclaw/token", {
    method: "POST",
  })
}

export async function setupMintClaw(): Promise<MintClawSetupResponse> {
  return request<MintClawSetupResponse>("/api/mintclaw/setup", {
    method: "POST",
  })
}

export type { MintClawInfoResponse, MintClawSetupResponse }

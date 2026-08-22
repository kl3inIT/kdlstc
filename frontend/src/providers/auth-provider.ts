import type { AuthProvider } from "@refinedev/core"

import { clearCsrfToken, getCsrfToken } from "@/lib/csrf"
import { fixtureMode } from "@/providers/data-provider"

type CurrentUser = { username: string; displayName?: string; roles?: string[] }

let fixtureAuthenticated = true

async function currentUser(): Promise<CurrentUser | null> {
  if (fixtureMode) {
    if (!fixtureAuthenticated) return null
    return {
      username: "nguyen.minh.an",
      displayName: "Nguyễn Minh An",
      roles: ["chuyen_vien"],
    }
  }

  const response = await fetch("/api/me", { credentials: "include", headers: { Accept: "application/json" } })
  if (!response.ok) return null
  return response.json() as Promise<CurrentUser>
}

export const authProvider: AuthProvider = {
  login: async () => {
    if (fixtureMode) {
      fixtureAuthenticated = true
      return { success: true, redirectTo: "/" }
    }
    window.location.assign("/oauth2/authorization/keycloak")
    return { success: true }
  },
  logout: async () => {
    if (fixtureMode) {
      fixtureAuthenticated = false
      return { success: true, redirectTo: "/login" }
    }
    const csrfToken = await getCsrfToken()
    const response = await fetch("/api/logout", {
      method: "POST",
      credentials: "include",
      headers: { [csrfToken.headerName]: csrfToken.token },
    })
    clearCsrfToken()
    return response.ok || response.status === 401
      ? { success: true, redirectTo: "/login" }
      : { success: false, error: new Error(`Đăng xuất thất bại: HTTP ${response.status}`) }
  },
  check: async () => {
    const user = await currentUser()
    return user ? { authenticated: true } : { authenticated: false, logout: true, redirectTo: "/login" }
  },
  getIdentity: currentUser,
  getPermissions: async () => (await currentUser())?.roles ?? [],
  onError: async (error) => {
    const status = (error as { status?: number }).status
    return status === 401 ? { logout: true, redirectTo: "/login", error } : { error }
  },
}

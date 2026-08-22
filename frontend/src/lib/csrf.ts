type CsrfToken = {
  headerName: string
  parameterName: string
  token: string
}

let cachedToken: Promise<CsrfToken> | undefined

export function getCsrfToken(): Promise<CsrfToken> {
  cachedToken ??= fetch("/api/csrf", {
    credentials: "include",
    headers: { Accept: "application/json" },
  }).then(async (response) => {
    if (!response.ok) {
      cachedToken = undefined
      throw new Error(`Không lấy được CSRF token: HTTP ${response.status}`)
    }
    return response.json() as Promise<CsrfToken>
  })
  return cachedToken
}

export function clearCsrfToken() {
  cachedToken = undefined
}

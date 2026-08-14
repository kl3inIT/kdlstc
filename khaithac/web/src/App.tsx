import { BotMessageSquare, LogOut, Moon, Sun, TableProperties } from "lucide-react"
import { useTheme } from "next-themes"
import { useEffect, useState } from "react"

import { Avatar, AvatarFallback } from "@/components/ui/avatar"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { BaoCao } from "@/features/baocao/BaoCao"
import { Chat } from "@/features/chat/Chat"

function ThemeToggle() {
  const { setTheme, resolvedTheme } = useTheme()
  return (
    <Button
      variant="ghost"
      size="icon"
      aria-label="Đổi giao diện sáng/tối"
      onClick={() => setTheme(resolvedTheme === "dark" ? "light" : "dark")}
    >
      <Sun className="size-4 scale-100 rotate-0 transition-all dark:scale-0 dark:-rotate-90" />
      <Moon className="absolute size-4 scale-0 rotate-90 transition-all dark:scale-100 dark:rotate-0" />
    </Button>
  )
}

export default function App() {
  const [tab, setTab] = useState("chat")
  const [username, setUsername] = useState<string>()

  useEffect(() => {
    fetch("/api/me", { headers: { Accept: "application/json" } }).then(async (r) => {
      if (r.redirected || r.status === 401) {
        location.href = "/oauth2/authorization/keycloak"
        return
      }
      if (r.ok) setUsername((await r.json()).username)
    })
  }, [])

  return (
    <div className="mx-auto flex h-dvh max-w-5xl flex-col gap-3 px-4 py-3">
      <header className="flex items-center justify-between gap-3 border-b pb-3">
        <div className="flex items-center gap-4">
          <h1 className="text-base font-semibold tracking-tight">
            Khai thác dữ liệu tài chính
          </h1>
          <Tabs value={tab} onValueChange={setTab}>
            <TabsList>
              <TabsTrigger value="chat" className="gap-1.5">
                <BotMessageSquare className="size-4" /> Trợ lý AI
              </TabsTrigger>
              <TabsTrigger value="baocao" className="gap-1.5">
                <TableProperties className="size-4" /> Báo cáo
              </TabsTrigger>
            </TabsList>
          </Tabs>
        </div>
        <div className="flex items-center gap-1">
          <ThemeToggle />
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" className="gap-2 px-2">
                <Avatar className="size-7">
                  <AvatarFallback className="text-xs uppercase">
                    {(username ?? "?").slice(0, 2)}
                  </AvatarFallback>
                </Avatar>
                <span className="hidden text-sm sm:inline">{username ?? "…"}</span>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuLabel>{username ?? "Chưa đăng nhập"}</DropdownMenuLabel>
              <DropdownMenuSeparator />
              <DropdownMenuItem
                onSelect={() => {
                  // Spring Security logout đòi POST — dùng form để browser
                  // theo được chuỗi redirect sang Keycloak (hủy cả phiên SSO).
                  const form = document.createElement("form")
                  form.method = "POST"
                  form.action = "/logout"
                  document.body.appendChild(form)
                  form.submit()
                }}
              >
                <LogOut className="size-4" /> Đăng xuất
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>
      </header>

      {/* Giữ cả hai panel mounted để không mất hội thoại khi đổi tab */}
      <main className="min-h-0 flex-1">
        <div className={tab === "chat" ? "flex h-full flex-col" : "hidden"}>
          <Chat />
        </div>
        <div className={tab === "baocao" ? "h-full" : "hidden"}>
          <BaoCao />
        </div>
      </main>
    </div>
  )
}

import { Authenticated, Refine } from "@refinedev/core"
import routerProvider from "@refinedev/react-router"
import { BrowserRouter, Navigate, Outlet, Route, Routes } from "react-router"
import { Activity, Database } from "lucide-react"

import { AppShell } from "@/components/app-shell"
import { LoginPage } from "@/pages/login-page"
import { OverviewPage } from "@/pages/overview-page"
import { authProvider } from "@/providers/auth-provider"
import { dataProvider } from "@/providers/data-provider"

export default function App() {
  return (
    <BrowserRouter>
      <Refine
        dataProvider={dataProvider}
        authProvider={authProvider}
        routerProvider={routerProvider}
        resources={[
          { name: "platform-overview", list: "/", meta: { label: "Tổng quan 7 bước", icon: <Database /> } },
          { name: "pipeline-runs", list: "/runs", meta: { label: "Lần chạy pipeline", icon: <Activity /> } },
        ]}
        options={{
          syncWithLocation: true,
          warnWhenUnsavedChanges: true,
          title: { text: "Kho dữ liệu iMate", icon: <Database className="size-5" /> },
        }}
      >
        <Routes>
          <Route
            element={
              <Authenticated key="private" redirectOnFail="/login">
                <AppShell />
              </Authenticated>
            }
          >
            <Route index element={<OverviewPage />} />
            <Route path="runs" element={<OverviewPage />} />
          </Route>
          <Route
            element={
              <Authenticated key="public" fallback={<Outlet />}>
                <Navigate to="/" replace />
              </Authenticated>
            }
          >
            <Route path="login" element={<LoginPage />} />
          </Route>
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Refine>
    </BrowserRouter>
  )
}

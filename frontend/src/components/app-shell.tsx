import { Outlet } from "react-router"

import { Layout } from "@/components/refine-ui/layout/layout"

export function AppShell() {
  return (
    <Layout>
      <Outlet />
    </Layout>
  )
}

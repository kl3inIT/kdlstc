import { useLogin } from "@refinedev/core"

import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardFooter, CardHeader, CardTitle } from "@/components/ui/card"

export function LoginPage() {
  const { mutate: login } = useLogin()

  return (
    <main className="flex min-h-screen items-center justify-center p-4">
      <Card className="w-full max-w-sm">
        <CardHeader>
          <CardTitle>Điều hành kho dữ liệu</CardTitle>
          <CardDescription>Đăng nhập bằng tài khoản định danh tập trung để tiếp tục.</CardDescription>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">Kho dữ liệu tổng hợp — Sở Tài chính Hưng Yên</p>
        </CardContent>
        <CardFooter>
          <Button className="w-full" onClick={() => login({})}>Đăng nhập qua SSO</Button>
        </CardFooter>
      </Card>
    </main>
  )
}

import { LoaderCircle, ShieldAlert, TriangleAlert } from "lucide-react"
import { useEffect, useState } from "react"

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Skeleton } from "@/components/ui/skeleton"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"

type BieuInfo = { ma_bieu: string; ten_bieu: string }
type Cot = { ten: string; nhan: string; bi_an: boolean }
type Row = { khoa: string; ten: string; gia_tri: Record<string, number | null> }
type KetQua = {
  ma_bieu: string
  ten_bieu: string
  tu_ngay: string
  den_ngay: string
  nguon_bi_an: string[]
  cot: Cot[]
  rows: Row[]
  khai_bao: string | null
}

function fmt(v: number | null | undefined): string {
  if (v === null || v === undefined) return "—"
  if (Math.abs(v) >= 1_000_000) return (v / 1_000_000).toLocaleString("vi-VN") + "tr"
  return Number.isInteger(v) ? v.toLocaleString("vi-VN") : v.toFixed(1)
}

export function BaoCao() {
  const [ds, setDs] = useState<BieuInfo[]>([])
  const [ma, setMa] = useState("")
  const [tu, setTu] = useState("2026-01-01")
  const [den, setDen] = useState("2026-06-30")
  const [kq, setKq] = useState<KetQua>()
  const [loi, setLoi] = useState<string>()
  const [dangTai, setDangTai] = useState(false)

  useEffect(() => {
    fetch("/api/bao-cao", { headers: { Accept: "application/json" } })
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error("HTTP " + r.status))))
      .then((d: BieuInfo[]) => {
        setDs(d)
        if (d.length > 0) setMa(d[0].ma_bieu)
      })
      .catch((e) => setLoi(String(e)))
  }, [])

  const dung = async () => {
    if (!ma) return
    setDangTai(true)
    setLoi(undefined)
    try {
      const r = await fetch(`/api/bao-cao/${ma}?tu_ngay=${tu}&den_ngay=${den}`)
      if (r.status === 401) {
        // Phiên hết hạn hẳn (refresh cũng không cứu được) → đăng nhập lại
        location.href = "/oauth2/authorization/keycloak"
        return
      }
      if (!r.ok) {
        const body = await r.json().catch(() => ({}))
        throw new Error(body.message ?? body.error ?? "HTTP " + r.status)
      }
      setKq(await r.json())
    } catch (e) {
      setKq(undefined)
      setLoi(e instanceof Error ? e.message : String(e))
    } finally {
      setDangTai(false)
    }
  }

  const cols = kq?.cot.filter((c) => !c.bi_an) ?? []
  const colsAn = kq?.cot.filter((c) => c.bi_an) ?? []

  return (
    <Card className="flex h-full flex-col overflow-hidden">
      <CardHeader className="border-b">
        <CardTitle>Dựng biểu báo cáo</CardTitle>
        <CardDescription>
          Biểu ghép từ API các app nguồn theo quyền của bạn — cột thiếu quyền tự ẩn.
        </CardDescription>
        <div className="flex flex-wrap items-center gap-2 pt-1">
          <Select value={ma} onValueChange={setMa}>
            <SelectTrigger className="w-90 max-w-full">
              <SelectValue placeholder="Chọn biểu…" />
            </SelectTrigger>
            <SelectContent>
              {ds.map((b) => (
                <SelectItem key={b.ma_bieu} value={b.ma_bieu}>
                  {b.ma_bieu} — {b.ten_bieu}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Input type="date" value={tu} onChange={(e) => setTu(e.target.value)} className="w-36" />
          <span className="text-sm text-muted-foreground">→</span>
          <Input type="date" value={den} onChange={(e) => setDen(e.target.value)} className="w-36" />
          <Button onClick={dung} disabled={dangTai || !ma}>
            {dangTai && <LoaderCircle className="size-4 animate-spin" />}
            Dựng biểu
          </Button>
        </div>
      </CardHeader>

      <CardContent className="min-h-0 flex-1 space-y-3 overflow-auto pt-4">
        {loi && (
          <Alert variant="destructive">
            <TriangleAlert className="size-4" />
            <AlertTitle>Lỗi dựng biểu</AlertTitle>
            <AlertDescription>{loi}</AlertDescription>
          </Alert>
        )}

        {kq?.khai_bao && (
          <Alert>
            <ShieldAlert className="size-4" />
            <AlertTitle>Thiếu quyền nguồn: {kq.nguon_bi_an.join(", ")}</AlertTitle>
            <AlertDescription>
              <p>{kq.khai_bao}</p>
              <div className="flex flex-wrap gap-1 pt-1">
                {colsAn.map((c) => (
                  <Badge key={c.ten} variant="secondary">
                    {c.nhan}
                  </Badge>
                ))}
              </div>
            </AlertDescription>
          </Alert>
        )}

        {dangTai && !kq && (
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-9 w-full" />
            ))}
          </div>
        )}

        {kq && (
          <>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Đối tượng</TableHead>
                  {cols.map((c) => (
                    <TableHead key={c.ten} className="text-right">
                      {c.nhan}
                    </TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {kq.rows.map((r) => (
                  <TableRow key={r.khoa}>
                    <TableCell className="font-medium">{r.ten}</TableCell>
                    {cols.map((c) => {
                      const v = r.gia_tri[c.ten]
                      return (
                        <TableCell
                          key={c.ten}
                          className={
                            "text-right tabular-nums " +
                            (typeof v === "number" && v < 0 ? "font-semibold text-destructive" : "")
                          }
                        >
                          {fmt(v)}
                        </TableCell>
                      )
                    })}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
            <p className="text-xs text-muted-foreground">
              {kq.ten_bieu} | Kỳ {kq.tu_ngay} → {kq.den_ngay}
              {kq.rows.length === 0 && " | Không có dòng dữ liệu (có thể toàn bộ nguồn bị ẩn)."}
            </p>
          </>
        )}
      </CardContent>
    </Card>
  )
}

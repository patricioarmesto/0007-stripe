import { useEffect, useState } from 'react'
import {
  Bar, BarChart, CartesianGrid, Pie, PieChart, ResponsiveContainer,
  Tooltip, XAxis, YAxis, Cell, Legend,
} from 'recharts'
import { fetchStats, listPayments, type DayTotal, type Payment, type StatusCount } from '../api'

const COLORES: Record<string, string> = {
  succeeded: '#10b981',
  processing: '#f59e0b',
  created: '#6366f1',
  failed: '#ef4444',
}

export default function Dashboard() {
  const [dias, setDias] = useState<DayTotal[]>([])
  const [estados, setEstados] = useState<StatusCount[]>([])
  const [pagos, setPagos] = useState<Payment[]>([])

  const cargar = () => {
    fetchStats().then(({ dias, estados }) => {
      setDias(dias)
      setEstados(estados)
    }).catch(() => {})
    listPayments().then(setPagos).catch(() => {})
  }

  useEffect(() => {
    cargar()
    const id = setInterval(cargar, 3000)
    return () => clearInterval(id)
  }, [])

  return (
    <section className="space-y-6">
      <h2 className="text-xl font-semibold text-slate-800">Dashboard</h2>
      <div className="grid gap-6 lg:grid-cols-2">
        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <h3 className="mb-3 text-sm font-medium text-slate-600">Total cobrado por día (ARS)</h3>
          <ResponsiveContainer width="100%" height={220}>
            <BarChart data={dias}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis dataKey="day" />
              <YAxis />
              <Tooltip />
              <Bar dataKey="total" fill="#4f46e5" />
            </BarChart>
          </ResponsiveContainer>
        </div>
        <div className="rounded-xl border border-slate-200 bg-white p-4">
          <h3 className="mb-3 text-sm font-medium text-slate-600">Pagos por estado</h3>
          <ResponsiveContainer width="100%" height={220}>
            <PieChart>
              <Pie data={estados} dataKey="count" nameKey="status" outerRadius={70} label>
                {estados.map((e) => <Cell key={e.status} fill={COLORES[e.status] ?? '#94a3b8'} />)}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>
      <div className="overflow-x-auto rounded-xl border border-slate-200 bg-white">
        <table className="w-full text-left text-sm">
          <thead className="bg-slate-100 text-slate-600">
            <tr>
              <th className="p-3">ID</th><th className="p-3">Email</th>
              <th className="p-3">Monto</th><th className="p-3">Estado</th>
              <th className="p-3">Reintentos</th>
            </tr>
          </thead>
          <tbody>
            {pagos.map((p) => (
              <tr key={p.id} className="border-t border-slate-100">
                <td className="p-3 font-mono text-xs">{p.id.slice(0, 8)}</td>
                <td className="p-3">{p.email}</td>
                <td className="p-3">${(p.amount / 100).toFixed(2)}</td>
                <td className="p-3">
                  <span className="rounded bg-slate-100 px-2 py-1 text-xs">{p.status}</span>
                </td>
                <td className="p-3">{p.attempts}</td>
              </tr>
            ))}
            {pagos.length === 0 && <tr><td className="p-4 text-slate-500" colSpan={5}>Todavía no hay pagos.</td></tr>}
          </tbody>
        </table>
      </div>
    </section>
  )
}

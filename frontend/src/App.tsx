import { useState } from 'react'
import Checkout from './components/Checkout'
import Dashboard from './components/Dashboard'
import BreakIt from './components/BreakIt'
import type { Payment } from './api'

export default function App() {
  const [tab, setTab] = useState<'checkout' | 'dashboard'>('checkout')
  const [ultimo, setUltimo] = useState<Payment | null>(null)

  return (
    <main className="min-h-screen bg-slate-50 p-6 sm:p-10">
      <div className="mx-auto max-w-5xl space-y-6">
        <header className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <h1 className="text-3xl font-bold text-slate-900">¿Cómo funciona Stripe?</h1>
            <p className="text-slate-600">Idempotencia, webhooks y reintentos · nivel avanzado</p>
          </div>
          <nav className="flex gap-2">
            <button
              className={`rounded px-4 py-2 text-sm ${tab === 'checkout' ? 'bg-slate-900 text-white' : 'bg-white text-slate-700'}`}
              onClick={() => setTab('checkout')}
            >Checkout</button>
            <button
              className={`rounded px-4 py-2 text-sm ${tab === 'dashboard' ? 'bg-slate-900 text-white' : 'bg-white text-slate-700'}`}
              onClick={() => setTab('dashboard')}
            >Dashboard</button>
          </nav>
        </header>

        {tab === 'checkout' ? (
          <Checkout onPago={setUltimo} />
        ) : (
          <Dashboard />
        )}
        <BreakIt />
        {ultimo && (
          <p className="text-xs text-slate-500">
            Último pago: <code>{ultimo.id}</code> · estado <code>{ultimo.status}</code>
          </p>
        )}
      </div>
    </main>
  )
}

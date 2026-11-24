import { useState } from 'react'
import Checkout from './components/Checkout'
import type { Payment } from './api'

export default function App() {
  const [pagos, setPagos] = useState<Payment[]>([])
  return (
    <main className="min-h-screen bg-slate-50 p-8">
      <div className="mx-auto max-w-5xl space-y-6">
        <header>
          <h1 className="text-3xl font-bold text-slate-900">¿Cómo funciona Stripe?</h1>
          <p className="text-slate-600">Mini Stripe didáctico · idempotencia, webhooks y reintentos</p>
        </header>
        <Checkout onPago={(p) => setPagos((actuales) => [p, ...actuales.filter((x) => x.id !== p.id)])} />
        <pre className="rounded bg-slate-900 p-4 text-xs text-emerald-300">{JSON.stringify(pagos, null, 2)}</pre>
      </div>
    </main>
  )
}

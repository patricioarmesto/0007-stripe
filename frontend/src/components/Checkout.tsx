import { useState } from 'react'
import { createPayment, simulateWebhook, type Payment } from '../api'

export default function Checkout({ onPago }: { onPago: (p: Payment) => void }) {
  const [amount, setAmount] = useState(12500)
  const [email, setEmail] = useState('ana@example.com')
  const [mensaje, setMensaje] = useState('')
  const [ultimo, setUltimo] = useState<Payment | null>(null)

  const pagar = async (doble: boolean) => {
    setMensaje('')
    try {
      const key = crypto.randomUUID()
      const requests = doble
        ? [createPayment({ amount, email }, key), createPayment({ amount, email }, key)]
        : [createPayment({ amount, email }, key)]
      const results = await Promise.all(requests)
      const ids = results.map((r) => r.payment.id)
      const mismoId = ids.every((id) => id === ids[0])
      setUltimo(results[0].payment)
      onPago(results[0].payment)
      setMensaje(
        doble
          ? `Dos requests, mismo Idempotency-Key, mismo id: ${mismoId ? 'sí ✅' : 'NO ❌'} (${ids[0]})`
          : `Pago ${results[0].created ? 'creado' : 'reutilizado'}: ${ids[0]}`,
      )
    } catch (e) {
      setMensaje(`Error: ${(e as Error).message}`)
    }
  }

  const confirmar = async () => {
    if (!ultimo) return setMensaje('Primero hacé un pago')
    try {
      const r = await simulateWebhook(ultimo.id)
      setMensaje(`Webhook del banco: ${r.event_id ?? ''} → processed=${r.processed}`)
      onPago(ultimo)
    } catch (e) {
      setMensaje(`Error: ${(e as Error).message}`)
    }
  }

  return (
    <section className="rounded-xl border border-slate-200 bg-white p-6 shadow-sm">
      <h2 className="mb-4 text-xl font-semibold text-slate-800">Checkout demo</h2>
      <div className="grid gap-4 sm:grid-cols-2">
        <label className="text-sm text-slate-600">
          Monto (centavos ARS)
          <input
            className="mt-1 w-full rounded border border-slate-300 px-3 py-2"
            type="number"
            value={amount}
            onChange={(e) => setAmount(Number(e.target.value))}
          />
        </label>
        <label className="text-sm text-slate-600">
          Email
          <input
            className="mt-1 w-full rounded border border-slate-300 px-3 py-2"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
          />
        </label>
      </div>
      <div className="mt-5 flex flex-wrap gap-3">
        <button className="rounded bg-indigo-600 px-4 py-2 text-white hover:bg-indigo-500" onClick={() => pagar(false)}>
          Pagar
        </button>
        <button className="rounded bg-amber-500 px-4 py-2 text-white hover:bg-amber-400" onClick={() => pagar(true)}>
          Doble click a propósito
        </button>
        <button className="rounded bg-emerald-600 px-4 py-2 text-white hover:bg-emerald-500" onClick={confirmar}>
          Simular webhook del banco
        </button>
      </div>
      {mensaje && <p className="mt-4 rounded bg-slate-50 p-3 text-sm text-slate-700">{mensaje}</p>}
      <p className="mt-3 text-xs text-slate-500">
        El pago se crea en <code>processing</code>. Recién se marca <code>succeeded</code> cuando llega el webhook firmado.
      </p>
    </section>
  )
}

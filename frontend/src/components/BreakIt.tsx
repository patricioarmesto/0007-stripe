import { useState } from 'react'
import { retryWebhook } from '../api'

export default function BreakIt() {
  const [paymentId, setPaymentId] = useState('pay_123')
  const [eventId, setEventId] = useState('evt_456')
  const [resultado, setResultado] = useState('')

  const probeFirma = async () => {
    const res = await fetch('/api/webhooks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'X-Signature': 'sha256=malo' },
      body: JSON.stringify({
        event_id: 'evt_falso', type: 'payment.succeeded', payment_id: paymentId,
      }),
    })
    setResultado(`Firma inválida → HTTP ${res.status} (esperábamos 401)`)
  }

  const probeRetry = async () => {
    const r = await retryWebhook({
      payment_id: paymentId, event_id: eventId, type: 'payment.succeeded',
    })
    setResultado(`Retry → HTTP ${r.status} ${JSON.stringify(r.body)}`)
  }

  return (
    <section className="rounded-xl border border-dashed border-amber-400 bg-amber-50 p-6">
      <h2 className="text-xl font-semibold text-amber-900">Cómo romperlo</h2>
      <ul className="mt-3 list-disc space-y-2 pl-5 text-sm text-amber-900">
        <li><strong>Doble click:</strong> dos requests con el mismo Idempotency-Key vuelven con el mismo id; Postgres tiene una sola fila.</li>
        <li><strong>Firma inválida:</strong> un POST a <code>/webhooks</code> con otra firma da 401 y el pago no cambia.</li>
        <li><strong>Webhook duplicado:</strong> el tercer intento con el mismo <code>event_id</code> devuelve <code>already_processed</code> y no publica de nuevo.</li>
      </ul>
      <div className="mt-4 flex flex-wrap items-end gap-3">
        <label className="text-xs text-amber-900">payment_id
          <input className="mt-1 block rounded border border-amber-300 px-2 py-1" value={paymentId} onChange={(e) => setPaymentId(e.target.value)} />
        </label>
        <label className="text-xs text-amber-900">event_id
          <input className="mt-1 block rounded border border-amber-300 px-2 py-1" value={eventId} onChange={(e) => setEventId(e.target.value)} />
        </label>
        <button className="rounded bg-rose-600 px-3 py-2 text-sm text-white" onClick={probeFirma}>Probar firma inválida</button>
        <button className="rounded bg-indigo-600 px-3 py-2 text-sm text-white" onClick={probeRetry}>Probar reintento</button>
      </div>
      {resultado && <p className="mt-3 text-sm font-medium text-amber-900">{resultado}</p>}
      <p className="mt-3 text-xs text-amber-800">
        Copiá el <code>payment_id</code> de la tabla y usá siempre el mismo <code>event_id</code>: si lo cambiás, no estás probando idempotencia.
      </p>
    </section>
  )
}

import { useState } from 'react'

function App() {
  const [query, setQuery] = useState('SELECT * FROM users')
  const [data, setData] = useState([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const runQuery = async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await fetch('http://localhost:3000/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query }),
      })
      const resultData = await res.json()
      if (!res.ok) {
        throw new Error(resultData.error || 'Query failed')
      }
      setData(resultData)
    } catch (err) {
      console.error("Failed to fetch:", err)
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  const columns = data.length > 0 ? Object.keys(data[0]) : []

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-50 p-8 font-sans">
      <div className="max-w-4xl mx-auto">
        
        <header className="mb-8 border-b border-zinc-800 pb-4">
          <h1 className="text-2xl font-semibold tracking-tight">Database Explorer</h1>
          <p className="text-zinc-400 text-sm mt-1">Connected to local middleware: <span className="text-emerald-400 font-mono">postgres (6001)</span></p>
        </header>

        <main>
          <div className="mb-6">
            <textarea
              className="w-full h-32 p-4 bg-zinc-900 border border-zinc-800 rounded text-mono text-sm focus:outline-none focus:border-zinc-600 transition-colors"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Enter SQL query here..."
            />
            <button 
              onClick={runQuery}
              disabled={loading}
              className="mt-4 px-6 py-2 bg-emerald-600 hover:bg-emerald-500 rounded font-medium disabled:opacity-50 transition-colors"
            >
              {loading ? 'Running...' : 'Run Query'}
            </button>
            {error && <div className="mt-4 text-red-400 text-sm">Error: {error}</div>}
          </div>

          <div className="bg-zinc-900 border border-zinc-800 rounded-lg overflow-hidden shadow-xl">
            <div className="px-6 py-4 border-b border-zinc-800 bg-zinc-900/50">
              <h2 className="font-medium">Results</h2>
            </div>
            
            {loading ? (
              <div className="p-6 text-zinc-500">Executing query...</div>
            ) : data.length === 0 ? (
              <div className="p-6 text-zinc-500">No results to display.</div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-left text-sm whitespace-nowrap">
                  <thead className="bg-zinc-950/50 text-zinc-400 border-b border-zinc-800">
                    <tr>
                      {columns.map(col => (
                        <th key={col} className="px-6 py-3 font-medium">{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-zinc-800">
                    {data.map((row, i) => (
                      <tr key={i} className="hover:bg-zinc-800/50 transition-colors">
                        {columns.map(col => (
                          <td key={col} className="px-6 py-4">{row[col]}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </main>

      </div>
    </div>
  )
}

export default App
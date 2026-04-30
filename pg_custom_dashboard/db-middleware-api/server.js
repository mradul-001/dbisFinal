const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');

const app = express();
app.use(cors());
app.use(express.json());

const pool = new Pool({
  user: 'mradul',
  host: 'localhost',
  database: 'postgres',
  password: '',
  port: 6001,
});

// Endpoint to run arbitrary queries against the local router
app.post('/api/query', async (req, res) => {
  try {
    const { query } = req.body;
    if (!query) {
      return res.status(400).json({ error: 'Query is required' });
    }
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: err.message || 'Database query failed' });
  }
});

const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Bridge API running on http://localhost:${PORT}`);
});
import React, { useState } from 'react';

interface Party {
  id: string;
  kycStatus: string;
  amlRisk: string;
  allowed: boolean;
}

export const ComplianceDashboard: React.FC = () => {
  const [parties, setParties] = useState<Party[]>([]);
  const [search, setSearch] = useState('');

  const riskColour = (level: string) =>
    ({ Low: '#22c55e', Medium: '#f59e0b', High: '#ef4444', Blocked: '#7f1d1d' }[level] ?? '#6b7280');

  return (
    <div style={{ padding: 24, fontFamily: 'Inter, sans-serif' }}>
      <h1 style={{ fontSize: 22, fontWeight: 700 }}>Compliance Dashboard</h1>
      <input
        value={search}
        onChange={e => setSearch(e.target.value)}
        placeholder="Search party ID..."
        style={{ padding: '8px 12px', width: 320, borderRadius: 6, border: '1px solid #d1d5db' }}
      />
      <table style={{ marginTop: 16, width: '100%', borderCollapse: 'collapse' }}>
        <thead>
          <tr style={{ background: '#f9fafb' }}>
            {['Party ID', 'KYC Status', 'AML Risk', 'Allowed'].map(h => (
              <th key={h} style={{ padding: '10px 14px', textAlign: 'left', fontSize: 13 }}>{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {parties.filter(p => p.id.includes(search)).map(p => (
            <tr key={p.id} style={{ borderTop: '1px solid #e5e7eb' }}>
              <td style={{ padding: '10px 14px', fontFamily: 'monospace', fontSize: 13 }}>{p.id}</td>
              <td style={{ padding: '10px 14px' }}>{p.kycStatus}</td>
              <td style={{ padding: '10px 14px', color: riskColour(p.amlRisk), fontWeight: 600 }}>{p.amlRisk}</td>
              <td style={{ padding: '10px 14px' }}>{p.allowed ? '✅' : '❌'}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};

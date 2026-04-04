import axios from 'axios';

export type KYCStatus = 'Pending' | 'Approved' | 'Rejected' | 'Expired';
export type RiskLevel = 'Low' | 'Medium' | 'High' | 'Blocked';

export interface ComplianceCheckResult {
  party: string;
  kycStatus: KYCStatus;
  amlRisk: RiskLevel;
  allowed: boolean;
  checkedAt: string;
}

export class ComplianceClient {
  constructor(
    private readonly baseUrl: string,
    private readonly token: string,
  ) {}

  async checkParty(partyId: string): Promise<ComplianceCheckResult> {
    const res = await axios.post(
      `${this.baseUrl}/v1/compliance/check`,
      { party: partyId },
      { headers: { Authorization: `Bearer ${this.token}` } },
    );
    return res.data;
  }

  async getKYCStatus(partyId: string): Promise<KYCStatus> {
    const res = await this.checkParty(partyId);
    return res.kycStatus;
  }

  async isAllowed(partyId: string): Promise<boolean> {
    const result = await this.checkParty(partyId);
    return result.allowed;
  }
}

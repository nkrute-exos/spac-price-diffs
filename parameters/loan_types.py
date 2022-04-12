class WL:
    def __init__(self, wl_balance):
        self._commitment = wl_balance
        self._wal = 0
        self._yield = 0
        self._total_pnl = 0
        self._moic = 1 + (self._total_pnl/self._commitment)


class ParisPassuLoan:
    def __init__(self, wl_balance, a_adv):
        self._commitment_a = wl_balance * a_adv
        self._commitment_b = wl_balance * (1 - a_adv)
        self._wal_a = 0
        self._wal_b = 0
        self._yield_a = 0
        self._yield_b = 0
        self._total_pnl_a = 0
        self._total_pnl_b = 0
        self._moic_a = 1 + (self._total_pnl_a / self._commitment_a)
        self._moic_b = 1 + (self._total_pnl_b / self._commitment_b)


class ParisPassuSecurization:
    def __init__(self, wl_balance, a_adv):
        self._commitment_a = wl_balance * a_adv
        self._commitment_b = wl_balance * (1 - a_adv)
        self._wal_a = 0
        self._wal_b = 0
        self._yield_a = 0
        self._yield_b = 0
        self._total_pnl_a = 0
        self._total_pnl_b = 0
        self._moic_a = 1 + (self._total_pnl_a / self._commitment_a)
        self._moic_b = 1 + (self._total_pnl_b / self._commitment_b)


class RevSeqSecurization:
    def __init__(self, wl_balance, a_adv):
        self._commitment_a = wl_balance * a_adv
        self._commitment_b = wl_balance * (1 - a_adv)
        self._wal_a = 0
        self._wal_b = 0
        self._yield_a = 0
        self._yield_b = 0
        self._total_pnl_a = 0
        self._total_pnl_b = 0
        self._moic_a = 1 + (self._total_pnl_a / self._commitment_a)
        self._moic_b = 1 + (self._total_pnl_b / self._commitment_b)


class RevSeqSecurizationSPACS:
    def __init__(self, wl_balance, a_adv):
        self._commitment_a = wl_balance * a_adv
        self._commitment_b = wl_balance * (1 - a_adv)
        self._wal_a = 0
        self._wal_b = 0
        self._yield_a = 0
        self._yield_b = 0
        self._total_pnl_a = 0
        self._total_pnl_b = 0
        self._moic_a = 1 + (self._total_pnl_a / self._commitment_a)
        self._moic_b = 1 + (self._total_pnl_b / self._commitment_b)
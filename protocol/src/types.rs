/// Every message type the bus can carry.
/// The bus never inspects this — it's for publishers and subscribers only.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MsgType {
    PriceTick = 1,
    OrderBookDelta = 2,
    Signal = 3,
    ExecutionReport = 4,
    RiskBreach = 5,
    Heartbeat = 6,
}

impl MsgType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(Self::PriceTick),
            2 => Some(Self::OrderBookDelta),
            3 => Some(Self::Signal),
            4 => Some(Self::ExecutionReport),
            5 => Some(Self::RiskBreach),
            6 => Some(Self::Heartbeat),
            _ => None,
        }
    }
}

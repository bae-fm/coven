use super::*;

pub(crate) fn realistic_history() -> History {
    let mut h = History::new();
    for m in 1..20 {
        h.all(
            0,
            0,
            add(
                m,
                if m < 3 {
                    MemberRole::Admin
                } else {
                    MemberRole::Member
                },
            ),
        );
        // Each installation registers itself (§10): the member's phone,
        // then a second device.
        h.all(m, u64::from(m), device(u64::from(m)));
        h.all(m, 100 + u64::from(m), device(100 + u64::from(m)));
    }
    for c in 0..10 {
        h.all(c as u8, c, make(c, &format!("Circle {c}")));
        h.all(c as u8, c, join(c, ((c + 1) % 10) as u8));
        h.all(c as u8, c, join(c, (10 + c) as u8));
    }
    for m in 17..20 {
        h.all(m, u64::from(m), make(u64::from(m), "Private notes"));
    }
    while h.entries.len() < 2_000 {
        let n = h.entries.len();
        let c = (n % 10) as u64;
        match n {
            600 | 1_200 | 1_800 => {
                let m = 16 + (n / 600) as u8;
                let past: Vec<_> = (0..n).collect();
                h.push(
                    m,
                    u64::from(m),
                    &past,
                    rename(u64::from(m), "Offline rename"),
                );
                h.push(0, 0, &past, remove(m));
            }
            _ if n.is_multiple_of(50) && n + 1 < 2_000 => {
                let past: Vec<_> = (0..n).collect();
                h.push(c as u8, c, &past, rename(c, &format!("Plans {n}")));
                let other = (c + 1) % 10;
                h.push(other as u8, other, &past, rename(c, &format!("Trips {n}")));
            }
            // Ana's temporary device registers itself; her phone removes it
            // 50 entries later.
            _ if n % 100 == 25 => h.all(0, 1_000 + n as u64, device(1_000 + n as u64)),
            _ if n % 100 == 75 => h.all(
                0,
                0,
                StoreChange::RemoveDevice {
                    device: DeviceId(1_000 + n as u64 - 50),
                },
            ),
            _ if n % 80 == 40 => h.all(0, 0, join(0, 12)),
            _ if n % 80 == 70 => h.all(0, 0, leave(0, 12)),
            _ => h.all(c as u8, c, rename(c, &format!("Circle {c}, edit {n}"))),
        }
    }
    h
}

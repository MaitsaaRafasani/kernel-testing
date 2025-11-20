import { db } from "./db";

export function sleep(ms: number) {
    return new Promise(resolve =>  setTimeout(resolve, ms))
}

export async function registerDevice(imei: string, family: string, target: number, companyID: number) {
    console.log('register device', imei)
    const deviceCode = family === 'concox' ? '1001' : '2002'

    const query = "INSERT INTO device (imei, company_id, identifier, device_code, device_code_valid, vendor_type, target, status, create_uid, write_uid) VALUES ($1, $2, $3, $4, true, 'Mc Easy', $5, 1, 1, 1) ON CONFLICT DO NOTHING"
    const values = [imei, companyID, 'stress-' + imei, deviceCode, target]
    await db.query(query, values)
}

export async function removeDevice(prefix: string) {
    const query = "DELETE FROM device WHERE identifier ILIKE $1";
    const values = [`stress-${prefix}%`];
    await db.query(query, values);
}
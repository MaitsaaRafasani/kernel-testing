
export type workerToMaster = {
    imei: string
    delete: boolean
}

export type masterToWorker = {
    imei: string,
    allowed: boolean
}

export interface BaseDevice {
    model: string
    loginPacket(imei: string): Buffer | string
    loginReplyPacket(data: string): boolean
    locationPacket(): Buffer
    modelPacket(): Buffer
}

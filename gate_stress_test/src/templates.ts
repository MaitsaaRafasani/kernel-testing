import { BaseDevice } from "./type"

export const devices: Record<string, BaseDevice> = {
    concox: {
        model: "x3",
        loginPacket(imei: string) {
            return Buffer.from("787811010" + imei +"200812c90410f40e0d0a", "hex")
        },
        loginReplyPacket(data: string) {
            return (/^78780501[0-9A-Fa-f]{4}[0-9A-Fa-f]{4}0d0a$/.test(data))
        },
        locationPacket() {
            return Buffer.from('7878222219080a102021c700c7a6200c18caa000d03301fe0a3e990096f40100010022f95d0d0a', 'hex')
        },
        modelPacket() {
            return Buffer.from('797900332100000000015b56455253494f4e5d4e5433375f47543831305f574141445f56332e315f3232303930372e313631380bed993e0d0a', 'hex')
        }
    },
    teltonika: {
        model: "fmb130",
        loginPacket(imei: string) {
            return Buffer.from("000F" + "3" + imei.split("").join("3"), "hex")
        },
        loginReplyPacket(data: string) {
            return (data == '000000000000000e0c010500000006676574766572010000a4c2')
        },
        locationPacket() {
            return Buffer.from("")
        },
        modelPacket() {
            return Buffer.from("")
        }
    }
}
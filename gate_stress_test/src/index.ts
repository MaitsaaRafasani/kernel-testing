import * as dotenv from "dotenv"
dotenv.config()

import cluster, { Worker } from "node:cluster"
import { createConnection } from "node:net"
import { masterToWorker, workerToMaster } from "./type"
import { devices } from "./templates"
import { sleep, registerDevice } from "./helper"


const host = process.env.TCP_HOST || "localhost"
const port = Number(process.env.TCP_PORT) || 1200

const workerCount = Number(process.env.WORKER) || 1
const clientPerWorker = Number(process.env.CLIENT_COUNTER) || 3
const generateDelay = (Number(process.env.CLIENT_DELAY) || 1) * 1000

const sendData = process.env.SEND_DATA === "true"
const dataDelay = (Number(process.env.DATA_DELAY) || 10) * 1000

const continuous = false
const liveDuration =
    (Number(process.env.LIVE_DURATION)) * 1000 || generateDelay * clientPerWorker

const regist = process.env.REGISTER_DEVICE === "true"

const imeiPrefix = process.env.IMEI_PREFIX || "198765"
const target = 1
const companyID = 1

const device = devices[process.env.DEVICE_MODEL || "concox"]

class RegisterWorker {
    constructor() {
        this.run()
    }

    async run() {
        const count = workerCount * clientPerWorker
        const imeis: string[] = []

        for (let i = 0; i < count; i++) {
            const seq = (i + 1).toString().padStart(9, "0")
            imeis.push(imeiPrefix + seq)
        }

        console.log(`RegisterWorker: registering ${imeis.length} devices`)

        for (const imei of imeis) {
            try {
                await registerDevice(
                    imei,
                    process.env.DEVICE_MODEL || "concox",
                    target,
                    companyID
                )
            } catch (err) {
                console.error(`RegisterWorker: failed for ${imei}`, err)
            }
        }

        console.log("RegisterWorker: completed.")
        process.send?.({ registerDone: true })
        process.exit(0)
    }
}


class TcpStressTest {
    imeiPool: string[] = []
    recycled: string[] = []
    poolIndex = 0

    async init() {
        const totalDevices = workerCount * clientPerWorker

        for (let i = 0; i < totalDevices; i++) {
            const seq = (i + 1).toString().padStart(9, "0")
            this.imeiPool.push(imeiPrefix + seq)
        }

        if (regist) {
            console.log("Primary: spawning registration worker...")

            const regWorker = cluster.fork({
                REGISTER_ONLY: "1"
            })

            regWorker.on("message", msg => {
                if (msg.registerDone) {
                    console.log("Primary: registration finished.")
                    this.startWorkers()
                }
            })

            return
        }

        this.startWorkers()
    }

    getNextImei(): string | undefined {
        if (this.recycled.length > 0) {
            return this.recycled.shift()
        }
        const imei = this.imeiPool[this.poolIndex]
        this.poolIndex++
        return imei
    }

    startWorkers() {
        console.log("Primary: starting stress workers...")

        const workers: Worker[] = []

        for (let i = 0; i < workerCount; i++) {
            const w = cluster.fork()
            workers.push(w)
        }

        for (const w of workers) {
            w.on("message", (msg: workerToMaster) => {
                if (msg.delete && msg.imei) {
                    this.recycled.push(msg.imei)
                    return
                }

                if ((msg as any).request) {
                    const imei = this.getNextImei()
                    if (!imei) w.send({ imei: "", allowed: false })
                    else w.send({ imei, allowed: true })
                    return
                }

                if (msg.imei) {
                    const alreadyUsed =
                        this.imeiPool.slice(0, this.poolIndex).includes(msg.imei) &&
                        !this.recycled.includes(msg.imei)

                    w.send({ imei: msg.imei, allowed: !alreadyUsed })
                }
            })
        }

        for (const w of workers) {
            for (let c = 0; c < clientPerWorker; c++) {
                const imei = this.getNextImei()
                if (!imei) w.send({ imei: "", allowed: false })
                else w.send({ imei, allowed: true })
            }
        }

        cluster.on("exit", worker => {
            console.log(`Worker ${worker.id} exited`)
        })
    }
}

class WorkerHandler {
    workerId = Number(cluster.worker!.id)
    activeConnections = 0
    totalConnected = 0

    constructor() {
        this.listen()
    }

    listen() {
        process.on("message", (msg: masterToWorker) => {
            if (msg.allowed) {
                if (msg.imei) this.runClient(msg.imei)
                else console.warn(`Worker ${this.workerId}: empty IMEI`)
            } else {
                this.requestImei()
            }
        })

        let generated = 0
        const timer = setInterval(() => {
            this.requestImei()
            generated++
            if (generated >= clientPerWorker && !continuous) {
                clearInterval(timer)
            }
        }, generateDelay)
    }

    requestImei() {
        process.send?.({ request: true })
    }

    async runClient(imei: string) {
        if (!device) {
            throw new Error(`Device model '${process.env.DEVICE_MODEL || "concox"}' not found`);
        }

        const client = createConnection({ host, port })
        let sendAllowed = false
        let sending = false
        let running = true

        client.on("connect", () => {
            client.write(device.loginPacket(imei))

            this.activeConnections++
            this.totalConnected++
            console.log(`Worker ${this.workerId}: active ${this.totalConnected}`)

            setTimeout(() => {
                client.destroy()
                running = false
            }, liveDuration)

            const loopSend = async () => {
                while (running) {
                    await sleep(dataDelay)
                    if (sendAllowed) {
                        sending = true
                        client.write(device.locationPacket())
                    }
                }
            }

            if (sendData) loopSend()
        })

        client.on("data", raw => {
            const hex = raw.toString("hex")

            if (device.loginReplyPacket(hex)) {
                sendAllowed = true
            }

            client.write(device.modelPacket())

            if (sending) sending = false
        })

        client.on("close", () => {
            console.log(`Worker ${this.workerId}: closed IMEI ${imei}`)

            this.activeConnections--
            process.send?.({ imei, delete: true })

            if (this.activeConnections === 0) {
                console.log(`Worker ${this.workerId} finished (${this.totalConnected} connections)`)
                process.exit(0)
            }
        })

        client.on("error", err => {
            console.error(`IMEI ${imei} error: ${err.message}`)
        })
    }
}


if (cluster.isPrimary) {
    const app = new TcpStressTest()
    app.init()
}
else if (process.env.REGISTER_ONLY === "1") {
    new RegisterWorker()
}
else {
    new WorkerHandler()
}

import { CosmosClient, Container, Database } from '@azure/cosmos'
import fs from 'fs'
import path from 'path'
import {
  syncItemToDRVault,
  syncItemsToDRVault,
  setSimulatedDROutage,
  getIsDROutageSimulated,
  getDRStatus,
} from './drVault'
import { buildSeedCollections } from './seedDataset'

export {
  syncItemToDRVault,
  syncItemsToDRVault,
  setSimulatedDROutage,
  getIsDROutageSimulated,
  getDRStatus,
}

let client: CosmosClient | null = null
let database: Database | null = null

export function isCosmosConfigured(): boolean {
  const endpoint = process.env.COSMOS_DB_ENDPOINT
  const key = process.env.COSMOS_DB_KEY
  return !!endpoint && !!key && !endpoint.includes('<your-account>') && !key.includes('<your-cosmos')
}

export function getCosmosDatabase(): Database | null {
  if (database) return database
  if (!isCosmosConfigured()) return null

  const endpoint = process.env.COSMOS_DB_ENDPOINT!
  const key = process.env.COSMOS_DB_KEY!
  const dbName = process.env.COSMOS_DB_DATABASE || 'KineticHR'

  client = new CosmosClient({
    endpoint,
    key,
    connectionPolicy: {
      requestTimeout: 4000,
    },
  })
  database = client.database(dbName)
  return database
}


const LOCAL_DB_FILE = path.resolve(__dirname, '../../../data/local-db.json')

function loadLocalDataFromDisk(): Record<string, any[]> {
  try {
    const dir = path.dirname(LOCAL_DB_FILE)
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true })
    }
    if (fs.existsSync(LOCAL_DB_FILE)) {
      const content = fs.readFileSync(LOCAL_DB_FILE, 'utf-8')
      const parsed = JSON.parse(content)
      if (parsed && typeof parsed === 'object' && Object.keys(parsed).length > 0) {
        return parsed
      }
    }
  } catch (err) {
    console.warn('Could not read local-db.json, falling back to seed generator:', err)
  }
  const seed = buildSeedCollections()
  saveLocalDataToDisk(seed)
  return seed
}

function saveLocalDataToDisk(data?: Record<string, any[]>) {
  try {
    const dataToSave = data || LOCAL_MEMORY_DATA
    const dir = path.dirname(LOCAL_DB_FILE)
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true })
    }
    fs.writeFileSync(LOCAL_DB_FILE, JSON.stringify(dataToSave, null, 2), 'utf-8')
  } catch (err) {
    console.warn('Failed to save to local-db.json:', err)
  }
}

const LOCAL_MEMORY_DATA: Record<string, any[]> = loadLocalDataFromDisk()

/**
 * Creates a mock Cosmos DB container for local offline development
 */
function createMockContainer(name: string): Container {
  if (!LOCAL_MEMORY_DATA[name]) {
    LOCAL_MEMORY_DATA[name] = []
  }
  const store = LOCAL_MEMORY_DATA[name]

  return {
    items: {
      query: (querySpec: any) => ({
        fetchAll: async () => {
          let results = [...store]
          const qStr = (querySpec?.query || '').toLowerCase()
          if (qStr.includes('c.status != "cancelled"') || qStr.includes("c.status != 'cancelled'")) {
            results = results.filter((item: any) => item.status !== 'cancelled')
          }
          if (qStr.includes('c.status = "cancelled"') || qStr.includes("c.status = 'cancelled'")) {
            results = results.filter((item: any) => item.status === 'cancelled')
          }

          if (querySpec && querySpec.parameters) {
            // Filter by tenantId
            const tenantParam = querySpec.parameters.find((p: any) => p.name === '@tenantId')
            if (tenantParam) {
              const tVal = String(tenantParam.value).toLowerCase()
              results = results.filter((item: any) => item.tenantId && item.tenantId.toLowerCase() === tVal)
            }
            // Filter by empId (employeeNumber, email, or id)
            const empParam = querySpec.parameters.find((p: any) => p.name === '@empId')
            if (empParam) {
              const eVal = String(empParam.value).toLowerCase()
              results = results.filter((u: any) =>
                (u.employeeNumber && u.employeeNumber.toLowerCase() === eVal) ||
                (u.email && u.email.toLowerCase() === eVal) ||
                (u.id && u.id.toLowerCase() === eVal)
              )
            }
            // Filter by userId (check userId, managerId, employeeId, or id)
            const userParam = querySpec.parameters.find((p: any) => p.name === '@userId')
            if (userParam) {
              const uVal = String(userParam.value).toLowerCase()
              results = results.filter((item: any) =>
                (item.userId && item.userId.toLowerCase() === uVal) ||
                (item.managerId && item.managerId.toLowerCase() === uVal) ||
                (item.employeeId && item.employeeId.toLowerCase() === uVal) ||
                (item.id && item.id.toLowerCase() === uVal)
              )
            }
            // Filter by managerId
            const mgrParam = querySpec.parameters.find((p: any) => p.name === '@managerId')
            if (mgrParam) {
              const mVal = String(mgrParam.value).toLowerCase()
              results = results.filter((item: any) => item.managerId && item.managerId.toLowerCase() === mVal)
            }
            // Filter by employeeId
            const empIdParam = querySpec.parameters.find((p: any) => p.name === '@employeeId')
            if (empIdParam) {
              const eIdVal = String(empIdParam.value).toLowerCase()
              results = results.filter((item: any) => item.employeeId && item.employeeId.toLowerCase() === eIdVal)
            }
            // If searching for organization code/id
            const orgParam = querySpec.parameters.find((p: any) => p.name === '@orgId')
            if (orgParam) {
              const val = String(orgParam.value).toLowerCase()
              results = results.filter(
                (o: any) => (o.code && o.code.toLowerCase() === val) || (o.id && o.id.toLowerCase() === val)
              )
            }
          }
          return { resources: results }
        },
      }),
      create: async (item: any) => {
        store.push(item)
        saveLocalDataToDisk()
        return { resource: item }
      },
      upsert: async (item: any) => {
        const idx = store.findIndex((i: any) => i.id === item.id)
        if (idx >= 0) store[idx] = item
        else store.push(item)
        saveLocalDataToDisk()
        return { resource: item }
      },
    },
    item: (id: string, _partitionKey?: string) => ({
      read: async () => {
        const item = store.find((i: any) => i.id === id)
        return { resource: item ? { ...item } : null }
      },
      replace: async (newItem: any) => {
        const idx = store.findIndex((i: any) => i.id === id)
        if (idx >= 0) store[idx] = newItem
        else store.push(newItem)
        saveLocalDataToDisk()
        return { resource: newItem }
      },
      delete: async () => {
        const idx = store.findIndex((i: any) => i.id === id)
        if (idx >= 0) store.splice(idx, 1)
        saveLocalDataToDisk()
        return { resource: {} }
      },
    }),
  } as unknown as Container
}

export function getTenantContainer(containerName: string): Container {
  const db = getCosmosDatabase()
  const mockContainer = createMockContainer(containerName)

  if (!db || !isCosmosConfigured()) {
    return mockContainer
  }

  const realContainer = db.container(containerName)
  const isStrictLive = (process.env.AZURE_MODE || '').toLowerCase() === 'live'

  const withTimeout = async <T>(promise: Promise<T>, ms = 2500): Promise<T> => {
    let timeoutHandle: any
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutHandle = setTimeout(() => reject(new Error(`Cosmos DB operation timed out after ${ms}ms`)), ms)
    })
    try {
      return await Promise.race([promise, timeoutPromise])
    } finally {
      clearTimeout(timeoutHandle)
    }
  }

  // Resilient wrapper: primary Azure Cosmos DB with DR vault sync & local failover
  return {
    items: {
      query: (querySpec: any, options?: any) => ({
        fetchAll: async () => {
          if (getIsDROutageSimulated()) {
            return mockContainer.items.query(querySpec).fetchAll()
          }

          try {
            const result = await withTimeout(realContainer.items.query(querySpec, options).fetchAll(), 3000)
            if (result.resources && result.resources.length > 0) {
              syncItemsToDRVault(containerName, result.resources)
            }
            return result
          } catch (err: any) {
            console.error(`[CosmosDB] Query failed on "${containerName}": ${err.message}`)
            return mockContainer.items.query(querySpec).fetchAll()
          }
        },
      }),
      create: async (item: any, options?: any) => {
        syncItemToDRVault(containerName, item)
        try { await mockContainer.items.create(item) } catch (_) {}

        if (getIsDROutageSimulated()) {
          return { resource: item }
        }

        try {
          return await withTimeout(realContainer.items.create(item, options), 3000)
        } catch (err: any) {
          console.error(`[CosmosDB] Insert failed on "${containerName}": ${err.message}`)
          return { resource: item }
        }
      },
      upsert: async (item: any, options?: any) => {
        syncItemToDRVault(containerName, item)
        try { await mockContainer.items.upsert(item) } catch (_) {}

        if (getIsDROutageSimulated()) {
          return { resource: item }
        }

        try {
          return await withTimeout(realContainer.items.upsert(item, options), 3000)
        } catch (err: any) {
          console.error(`[CosmosDB] Upsert failed on "${containerName}": ${err.message}`)
          return { resource: item }
        }
      },
    },
    item: (id: string, partitionKey?: string) => ({
      read: async () => {
        if (getIsDROutageSimulated()) {
          return mockContainer.item(id, partitionKey).read()
        }

        try {
          const result = await withTimeout(realContainer.item(id, partitionKey).read(), 2500)
          if (result.resource) {
            syncItemToDRVault(containerName, result.resource)
            return result
          }
          const mockResult = await mockContainer.item(id, partitionKey).read()
          if (mockResult.resource) {
            return mockResult
          }
          return result
        } catch (err: any) {
          console.error(`[CosmosDB] Read "${id}" failed on "${containerName}": ${err.message}`)
          return mockContainer.item(id, partitionKey).read()
        }
      },
      replace: async (newItem: any, options?: any) => {
        try {
          const result = await withTimeout(realContainer.item(id, partitionKey).replace(newItem, options), 2500)
          try { await mockContainer.item(id, partitionKey).replace(newItem) } catch (_) {}
          return result
        } catch (err: any) {
          console.error(`[CosmosDB] Replace "${id}" failed on "${containerName}": ${err.message}`)
          if (isStrictLive) {
            throw new Error(`Azure Cosmos DB Live Replace Failed: ${err.message}`)
          }
          return mockContainer.item(id, partitionKey).replace(newItem)
        }
      },
      delete: async () => {
        try {
          const result = await withTimeout(realContainer.item(id, partitionKey).delete(), 2500)
          try { await mockContainer.item(id, partitionKey).delete() } catch (_) {}
          return result
        } catch (err: any) {
          console.error(`[CosmosDB] Delete "${id}" failed on "${containerName}": ${err.message}`)
          if (isStrictLive) {
            throw new Error(`Azure Cosmos DB Live Delete Failed: ${err.message}`)
          }
          return mockContainer.item(id, partitionKey).delete()
        }
      },
    }),
  } as unknown as Container
}

/**
 * Executes a tenant-isolated query in Cosmos DB, with terminal indicators and fallback to local memory if needed.
 */
export async function queryTenantItems<T>(
  containerName: string,
  tenantId: string,
  query: string,
  parameters: Array<{ name: string; value: any }> = []
): Promise<T[]> {
  const container = getTenantContainer(containerName)
  const hasTenantParam = parameters.some(p => p.name === '@tenantId')
  const finalParams = hasTenantParam ? parameters : [...parameters, { name: '@tenantId', value: tenantId }]
  const querySpec = { query, parameters: finalParams }

  const { resources } = await container.items.query<T>(querySpec, { partitionKey: tenantId } as any).fetchAll()
  return (resources || []) as T[]
}

/**
 * Inserts or upserts a tenant item
 */
export async function createTenantItem<T extends { id?: string; tenantId: string }>(
  containerName: string,
  item: T
): Promise<T> {
  const container = getTenantContainer(containerName)
  const { resource } = await container.items.upsert<T>(item, { partitionKey: item.tenantId } as any)
  return (resource || item) as T
}

/**
 * Reads and updates a tenant item with a mutator callback
 */
export async function updateTenantItem<T extends { id: string; tenantId: string }>(
  containerName: string,
  id: string,
  tenantId: string,
  updateFn: (item: T) => T
): Promise<T> {
  const container = getTenantContainer(containerName)
  const { resource } = await container.item(id, tenantId).read<T>()
  if (!resource) {
    throw new Error(`Item "${id}" not found in container "${containerName}" for tenant "${tenantId}".`)
  }
  const updatedItem = updateFn(resource)
  const { resource: replaced } = await container.item(id, tenantId).replace<T>(updatedItem)
  return (replaced || updatedItem) as T
}


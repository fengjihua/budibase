import { get } from "svelte/store"

const SuppressErrors = true

export const createActions = context => {
  const { definition, API, datasource } = context

  const refreshDefinition = async () => {
    definition.set(await API.fetchTableDefinition(get(datasource).tableId))
  }

  const saveDefinition = async newDefinition => {
    await API.saveTable(newDefinition)
  }

  const saveRow = async row => {
    row.tableId = get(datasource)?.tableId
    return await API.saveRow(row, SuppressErrors)
  }

  const deleteRows = async rows => {
    await API.deleteRows({
      tableId: get(datasource).tableId,
      rows,
    })
  }

  const isDatasourceValid = datasource => {
    return datasource?.type === "table" && datasource?.tableId
  }

  const getRow = async id => {
    const res = await API.searchTable({
      tableId: get(datasource).tableId,
      limit: 1,
      query: {
        equal: {
          _id: id,
        },
      },
      paginate: false,
    })
    return res?.rows?.[0]
  }

  return {
    table: {
      actions: {
        refreshDefinition,
        saveDefinition,
        addRow: saveRow,
        updateRow: saveRow,
        deleteRows,
        getRow,
        isDatasourceValid,
      },
    },
  }
}

export const initialise = context => {
  const { datasource, fetch, filter, sort, table } = context

  // Keep a list of subscriptions so that we can clear them when the datasource
  // config changes
  let unsubscribers = []

  // Observe datasource changes and apply logic for table datasources
  datasource.subscribe($datasource => {
    // Clear previous subscriptions
    unsubscribers?.forEach(unsubscribe => unsubscribe())
    unsubscribers = []
    if (!table.actions.isDatasourceValid($datasource)) {
      return
    }

    // Wipe state
    filter.set([])
    sort.set({
      column: null,
      order: "ascending",
    })

    // Update fetch when filter changes
    unsubscribers.push(
      filter.subscribe($filter => {
        // Ensure we're updating the correct fetch
        const $fetch = get(fetch)
        if ($fetch?.options?.datasource?.tableId !== $datasource.tableId) {
          return
        }
        $fetch.update({
          filter: $filter,
        })
      })
    )

    // Update fetch when sorting changes
    unsubscribers.push(
      sort.subscribe($sort => {
        // Ensure we're updating the correct fetch
        const $fetch = get(fetch)
        if ($fetch?.options?.datasource?.tableId !== $datasource.tableId) {
          return
        }
        $fetch.update({
          sortOrder: $sort.order || "ascending",
          sortColumn: $sort.column,
        })
      })
    )
  })
}

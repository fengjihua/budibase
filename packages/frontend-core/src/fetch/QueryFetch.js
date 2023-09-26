import DataFetch from "./DataFetch.js"
import { Helpers } from "@budibase/bbui"
import { get } from "svelte/store"

export default class QueryFetch extends DataFetch {
  determineFeatureFlags(definition) {
    const supportsPagination =
      !!definition?.fields?.pagination?.type &&
      !!definition?.fields?.pagination?.location &&
      !!definition?.fields?.pagination?.pageParam
    return { supportsPagination }
  }

  async getDefinition(datasource) {
    if (!datasource?._id) {
      return null
    }
    try {
      let definition = await this.API.fetchQueryDefinition(datasource._id)
      // After getting the definition of query, it loses "fields" attribute
      // because of security reason from the server. However, this attribute
      // needs to be inside the definition for pagination.
      if (!definition.fields) {
        definition.fields = datasource.fields
      }
      await this.enrichPagination(definition)
      return definition
    } catch (error) {
      return null
    }
  }

  async enrichPagination(definition) {
    // console.log("enrichPagination", this.options)
    const { datasource, limit, paginate } = this.options
    if (datasource && paginate) {
      const datasources = await this.API.getDatasources()
      const ds = datasources.find(ds => ds._id === definition.datasourceId)
      if (ds && ds.source && ds.source === "MONGODB") {
        if (!definition.fields) {
          definition.fields = {}
        }
        if (!definition.fields.pagination) {
          definition.fields.pagination = {}
        }
        // change supportsPagination: true
        definition.fields.pagination = {
          type: "page",
          location: "query",
          pageParam: {
            limit,
          },
        }
      }
    }
  }

  async getData() {
    const { datasource, limit, paginate } = this.options
    const { supportsPagination } = this.features
    const { cursor, definition } = get(this.store)
    const type = definition?.fields?.pagination?.type

    // console.log("getData -> definition", definition)

    // Set the default query params
    let parameters = Helpers.cloneDeep(datasource?.queryParams || {})
    for (let param of datasource?.parameters || {}) {
      if (!parameters[param.name]) {
        parameters[param.name] = param.default
      }
    }

    // Add pagination to query if supported
    let queryPayload = { queryId: datasource?._id, parameters }
    if (paginate && supportsPagination) {
      const requestCursor = type === "page" ? parseInt(cursor || 1) : cursor
      queryPayload.pagination = { page: requestCursor, limit }
    }

    // Execute query
    try {
      const res = await this.API.executeQuery(queryPayload)
      const { data, pagination, ...rest } = res

      // console.log("getData -> executeQuery", res)

      // Derive pagination info from response
      let nextCursor = null
      let hasNextPage = false
      if (paginate && supportsPagination) {
        if (type === "page") {
          // For "page number" pagination, increment the existing page number
          nextCursor = queryPayload.pagination.page + 1
          hasNextPage = data?.length === limit && limit > 0
        } else {
          // For "cursor" pagination, the cursor should be in the response
          nextCursor = pagination?.cursor
          hasNextPage = nextCursor != null
        }
      }

      return {
        rows: data || [],
        info: rest,
        cursor: nextCursor,
        hasNextPage,
      }
    } catch (error) {
      return {
        rows: [],
        hasNextPage: false,
      }
    }
  }
}

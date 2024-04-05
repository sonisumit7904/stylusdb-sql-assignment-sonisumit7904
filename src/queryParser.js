function parseDeleteQuery(query) {
  query = query.trim();

  // Where Clause logic
  const whereSplit = query.split(/\sWHERE\s/i);
  query = whereSplit[0]; // Everything before WHERE clause
  const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;
  let whereClauses = [];
  if (whereClause) {
    whereClauses = parseWhereClause(whereClause);
  }

  //insert regex
  const deleteRegex = /^DELETE FROM\s+(\w+)/i;
  const deleteMatch = query.match(deleteRegex);

  if (!deleteMatch) {
    throw new Error("Invalid DELETE format");
  }

  const [, table] = deleteMatch;

  return {
    type: "DELETE",
    table: table.trim(),
    whereClauses: whereClauses,
  };
}

function parseINSERTQuery(query) {
  query = query.trim();

  //insert regex
  const insertPattern =
    /^INSERT INTO\s+(\w+)\s+\(([^)]+)\)\s+VALUES\s+\(([^)]+)\)/i;

  const insertMatch = query.match(insertPattern);

  if (!insertMatch) {
    throw new Error("Invalid INSERT format");
  }

  const [, table, fields, values] = insertMatch;
  return {
    type: "INSERT",
    table: table.trim(),
    columns: fields.split(",").map((field) => removeQuotes(field.trim())),
    values: values.split(",").map((val) => removeQuotes(val.trim())),
  };
}

function parseSelectQuery(query) {
  try {
    // First, let's trim the query to remove any leading/trailing whitespaces
    query = query.trim();

    let isDistinct = false;
    if (query.toUpperCase().includes("SELECT DISTINCT")) {
      isDistinct = true;
      query = query.replace("SELECT DISTINCT", "SELECT");
    }

    // Updated regex to capture LIMIT clause
    const limitRegex = /\sLIMIT\s(\d+)/i;
    const limitMatch = query.match(limitRegex);

    let limit = null;
    if (limitMatch) {
      query = query.split(/\sLIMIT\s/i)[0];
      limit = parseInt(limitMatch[1]);
    }

    // Updated regex to capture ORDER BY clause
    const orderByRegex = /\sORDER BY\s(.+)/i;
    const orderByMatch = query.match(orderByRegex);

    let orderByFields = null;
    if (orderByMatch) {
      query = query.split(/\sORDER BY\s/i)[0];
      orderByFields = orderByMatch[1].split(",").map((field) => {
        const [fieldName, order] = field.trim().split(/\s+/);
        return { fieldName, order: order ? order.toUpperCase() : "ASC" };
      });
    }

    // Updated regex to capture GROUP BY clause
    const groupByRegex = /\sGROUP BY\s(.+)/i;
    const groupByMatch = query.match(groupByRegex);

    let groupByFields = null;

    if (groupByMatch) {
      groupByFields = groupByMatch[1].split(",").map((field) => field.trim());
      query = query.split(/\sGROUP BY\s/i)[0];
    }

    const aggRegex = /\s(COUNT|AVG|MAX|MIN|SUM)\(.*?\)/gi;
    const aggMatch = query.match(aggRegex);

    let hasAggregateWithoutGroupBy = false;
    if (!groupByMatch && aggMatch) {
      hasAggregateWithoutGroupBy = true;
    }

    // Initialize variables for different parts of the query
    let selectPart, fromPart;

    // Split the query at the WHERE clause if it exists
    const whereSplit = query.split(/\sWHERE\s/i);
    query = whereSplit[0]; // Everything before WHERE clause

    // WHERE clause is the second part after splitting, if it exists
    const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

    // Split the remaining query at the JOIN clause if it exists
    const joinSplit = query.split(/\s(INNER|LEFT|RIGHT) JOIN\s/i);
    selectPart = joinSplit[0].trim(); // Everything before JOIN clause

    // Parse the SELECT part
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
    const selectMatch = selectPart.match(selectRegex);
    if (!selectMatch) {
      throw new Error("Invalid SELECT format");
    }

    const [, fields, table] = selectMatch;

    // Parse the JOIN part if it exists
    const { joinType, joinTable, joinCondition } = parseJoinClause(query);

    // Parse the WHERE part if it exists
    let whereClauses = [];
    if (whereClause) {
      whereClauses = parseWhereClause(whereClause);
    }

    return {
      fields: fields.split(",").map((field) => field.trim()),
      table: table.trim(),
      whereClauses,
      joinType,
      joinTable,
      joinCondition,
      groupByFields,
      hasAggregateWithoutGroupBy,
      orderByFields,
      limit,
      isDistinct,
    };
  } catch (error) {
    // console.error(error);
    throw new Error(`Query parsing error: ${error.message}`);
  }
}

function parseWhereClause(whereString) {
  try {
    const conditionRegex = /(.*?)(=|!=|>|<|>=|<=)(.*)/;
    return whereString.split(/ AND | OR /i).map((conditionString) => {
      const match = conditionString.match(conditionRegex);

      if (conditionString.includes(" LIKE ")) {
        const [field, pattern] = conditionString.split(/\sLIKE\s/i);
        return {
          field: field.trim(),
          operator: "LIKE",
          value: removeQuotes(pattern.trim()),
        };
      }

      if (match) {
        const [, field, operator, value] = match;

        // to remove cases of 'value'

        return {
          field: field.trim(),
          operator,
          value: value.trim(),
        };
      }
      throw new Error("Invalid WHERE clause format");
    });
  } catch (error) {
    throw error;
  }
}

function parseJoinClause(query) {
  const joinRegex =
    /\s(INNER|LEFT|RIGHT) JOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
  const joinMatch = query.match(joinRegex);

  if (joinMatch) {
    return {
      joinType: joinMatch[1].trim(),
      joinTable: joinMatch[2].trim(),
      joinCondition: {
        left: joinMatch[3].trim(),
        right: joinMatch[4].trim(),
      },
    };
  }

  return {
    joinType: null,
    joinTable: null,
    joinCondition: null,
  };
}

function removeQuotes(value) {
  if (
    typeof value === "string" &&
    (value.startsWith(`'`) || value.startsWith(`"`)) &&
    (value.endsWith(`'`) || value.endsWith(`"`))
  ) {
    return value.slice(1, -1); // Remove quotes
  }
  return value;
}

module.exports = {
  parseSelectQuery,
  parseJoinClause,
  removeQuotes,
  parseINSERTQuery,
  parseDeleteQuery,
};

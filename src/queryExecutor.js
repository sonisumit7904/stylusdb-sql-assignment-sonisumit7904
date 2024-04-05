// src/index.js
const fs = require("fs");
const {
  parseSelectQuery,
  removeQuotes,
  parseINSERTQuery,
  parseDeleteQuery,
} = require("./queryParser");
const { readCSV, writeCSV } = require("./csvReader");

function evaluateCondition(row, clause) {
  const { field, operator, value } = clause;
  const newValue = removeQuotes(value);
  switch (operator) {
    case "=":
      return row[field] === newValue;
    case "!=":
      return row[field] !== newValue;
    case ">":
      return row[field] > newValue;
    case "<":
      return row[field] < newValue;
    case ">=":
      return row[field] >= newValue;
    case "<=":
      return row[field] <= newValue;
    case "LIKE":
      const regexPattern = "^" + clause.value.replace(/%/g, ".*") + "$";
      return new RegExp(regexPattern, "i").test(row[clause.field]);
    default:
      throw new Error(`Unsupported operator: ${operator}`);
  }
}

function aggregatedOperations(aggregateFunction, rows) {
  const [op, fieldName] = aggregateFunction
    .split("(")
    .map((part) => part.trim().replace(")", ""));
  if (fieldName === "*") {
    return rows.length;
  }

  const values = rows.map((row) => row[fieldName]);

  let result;
  switch (op.toUpperCase()) {
    case "COUNT":
      result = values.length;
      break;
    case "AVG":
      result =
        values.reduce((acc, val) => acc + Number(val), 0) / values.length;
      break;
    case "MAX":
      result = Math.max(...values);
      break;
    case "MIN":
      result = Math.min(...values);
      break;
    case "SUM":
      result = values.reduce((acc, val) => acc + Number(val), 0);
      break;
    // Handle other aggregate functions if needed
    default:
      throw new Error(`Unsupported aggregate function: ${op}`);
  }

  return result;
}

// Helper function to apply GROUP BY and aggregate functions
function applyGroupBy(data, groupByFields, aggregateFunctions) {
  const groupedData = data.reduce((acc, row) => {
    //generate a group key from group by fields for each distinct possible group
    const groupKey = groupByFields.map((field) => row[field]).join("_");

    if (!acc[groupKey]) {
      acc[groupKey] = [];
    }
    //group the rows according to the groupKey
    acc[groupKey].push(row);

    return acc;
  }, {});

  const aggregatedData = Object.entries(groupedData).map(
    ([groupKey, groupRows]) => {
      const group = {};

      const groupRowValues = groupKey.split("_");
      groupByFields.forEach((field, index) => {
        group[field] = groupRowValues[index];
        aggregateFunctions = aggregateFunctions.filter((func) => {
          return func !== field;
        });
      });

      aggregateFunctions.forEach((func) => {
        group[`${func}`] = aggregatedOperations(func, groupRows);
      });

      return group;
    }
  );

  return aggregatedData;
}

// Helper functions for different JOIN types
function performInnerJoin(data, joinData, joinCondition, fields, table) {
  data = data.flatMap((mainRow) => {
    return joinData
      .filter((joinRow) => {
        const mainValue = mainRow[joinCondition.left.split(".")[1]];
        const joinValue = joinRow[joinCondition.right.split(".")[1]];
        return mainValue === joinValue;
      })
      .map((joinRow) => {
        return fields.reduce((acc, field) => {
          const [tableName, fieldName] = field.split(".");
          acc[field] =
            tableName === table ? mainRow[fieldName] : joinRow[fieldName];
          return acc;
        }, {});
      });
  });

  return data;
}

function performLeftJoin(data, joinData, joinCondition, fields, table) {
  data = data.flatMap((mainRow) => {
    const matchingRows = joinData.filter((joinRow) => {
      const mainValue = mainRow[joinCondition.left.split(".")[1]];
      const joinValue = joinRow[joinCondition.right.split(".")[1]];
      return mainValue === joinValue;
    });

    //no matching row found for particular main row
    if (matchingRows.length == 0) {
      return fields.reduce((acc, field) => {
        const [tablename, fieldname] = field.split(".");
        acc[field] = tablename === table ? mainRow[fieldname] : null;
        return acc;
      }, {});
    }

    //matching rows exist
    return matchingRows.map((joinRow) => {
      return fields.reduce((acc, field) => {
        const [tableName, fieldName] = field.split(".");
        acc[field] =
          tableName === table ? mainRow[fieldName] : joinRow[fieldName];
        return acc;
      }, {});
    });
  });

  return data;
}

function performRightJoin(data, joinData, joinCondition, fields, table) {
  joinData = joinData.flatMap((joinRow) => {
    const matchingRows = data.filter((mainRow) => {
      const mainValue = mainRow[joinCondition.left.split(".")[1]];
      const joinValue = joinRow[joinCondition.right.split(".")[1]];
      return mainValue === joinValue;
    });

    //matching values dont exist for particular join row
    if (matchingRows.length == 0) {
      return fields.reduce((acc, field) => {
        const [tablename, fieldname] = field.split(".");
        acc[field] = tablename === table ? null : joinRow[fieldname];
        return acc;
      }, {});
    }

    //matching rows exist
    return matchingRows.map((mainRow) => {
      return fields.reduce((acc, field) => {
        const [tablename, fieldname] = field.split(".");
        acc[field] =
          tablename === table ? mainRow[fieldname] : joinRow[fieldname];
        return acc;
      }, {});
    });
  });

  return joinData;
}

async function executeSELECTQuery(query) {
  try {
    const {
      fields,
      table,
      whereClauses,
      joinType,
      joinTable,
      joinCondition,
      groupByFields,
      hasAggregateWithoutGroupBy,
      orderByFields,
      limit,
      isDistinct,
    } = parseSelectQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Logic for applying JOINs
    if (joinTable && joinCondition) {
      const joinData = await readCSV(`${joinTable}.csv`);

      const dataKeys = data
        ? Object.keys(data[0]).map((key) => `${table}.${key}`)
        : [];
      const joinDataKeys = joinData
        ? Object.keys(joinData[0]).map((key) => `${joinTable}.${key}`)
        : [];
      const allFields = [...dataKeys, ...joinDataKeys];

      switch (joinType.toUpperCase()) {
        case "INNER":
          data = performInnerJoin(
            data,
            joinData,
            joinCondition,
            allFields,
            table
          );
          break;
        case "LEFT":
          data = performLeftJoin(
            data,
            joinData,
            joinCondition,
            allFields,
            table
          );
          break;
        case "RIGHT":
          data = performRightJoin(
            data,
            joinData,
            joinCondition,
            allFields,
            table
          );
          break;
        // Handle default case or unsupported JOIN types
        default:
          throw new Error(`Unsupported JOIN type: ${joinType.ToUpperCase()}`);
      }
    }

    // logic for WHERE clause
    let filteredData =
      whereClauses.length > 0
        ? data.filter((row) =>
            whereClauses.every((clause) => evaluateCondition(row, clause))
          )
        : data;

    // logic for group by
    if (groupByFields) {
      filteredData = applyGroupBy(filteredData, groupByFields, fields);
    }

    if (hasAggregateWithoutGroupBy && fields.length == 1) {
      const selectedRow = {};
      selectedRow[fields[0]] = aggregatedOperations(fields[0], filteredData);
      return [selectedRow];
    }

    // console.log("AFTER GROUP: ", filteredData);

    if (orderByFields) {
      filteredData.sort((a, b) => {
        for (let { fieldName, order } of orderByFields) {
          if (a[fieldName] < b[fieldName]) return order === "ASC" ? -1 : 1;
          if (a[fieldName] > b[fieldName]) return order === "ASC" ? 1 : -1;
        }
        return 0;
      });
    }

    // console.log("AFTER ORDER: ", filteredData);

    if (limit !== null) {
      filteredData = filteredData.slice(0, limit);
    }

    if (isDistinct) {
      filteredData = [
        ...new Map(
          filteredData.map((item) => [
            fields.map((field) => item[field]).join("|"),
            item,
          ])
        ).values(),
      ];
    }

    // Filter the fields based on the query fields
    return filteredData.map((row) => {
      const selectedRow = {};
      fields.forEach((field) => {
        if (hasAggregateWithoutGroupBy) {
          selectedRow[field] = aggregatedOperations(field, filteredData);
        } else {
          selectedRow[field] = row[field];
        }
      });
      return selectedRow;
    });
  } catch (error) {
    throw new Error(`Error executing query: ${error.message}`);
  }
}

async function executeINSERTQuery(query) {
  const { type, table, columns, values } = parseINSERTQuery(query);

  if (type !== "INSERT") {
    throw new Error("Invalid INSERT format");
  }

  let orderedFields = columns;
  let existingData = [];

  if (fs.existsSync(`${table}.csv`)) {
    // Read existing data from CSV file
    existingData = await readCSV(`${table}.csv`);
    orderedFields =
      existingData.length >= 1 ? Object.keys(existingData[0]) : columns;
  }

  const data = [
    orderedFields.reduce((acc, field, index) => {
      acc[field] = values[index];
      return acc;
    }, {}),
  ];

  const newData = existingData.concat(data);

  await writeCSV(`${table}.csv`, newData);
}

// src/queryExecutor.js

async function executeDELETEQuery(query) {
  const { table, whereClauses } = parseDeleteQuery(query);

  let data = await readCSV(`${table}.csv`);

  const filteredData =
    whereClauses.length > 0
      ? data.filter((row) =>
          whereClauses.every((clause) => !evaluateCondition(row, clause))
        )
      : [];

  // Save the updated data back to the CSV file
  await writeCSV(`${table}.csv`, filteredData);

  return { message: "Rows deleted successfully." };
}

module.exports = { executeSELECTQuery, executeINSERTQuery, executeDELETEQuery };

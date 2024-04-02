// src/index.js

// const parseQuery = require("./queryParser");
// const readCSV = require("./csvReader");

// async function executeSELECTQuery(query) {
//   // const { fields, table, whereClauses } = parseQuery(query);
//   // const data = await readCSV(`${table}.csv`);

//   // src/index.js at executeSELECTQuery

//   // Now we will have joinTable, joinCondition in the parsed query
//   const { fields, table, whereClauses, joinTable, joinCondition } =
//     parseQuery(query);
//   data = await readCSV(`${table}.csv`);

//   // Perform INNER JOIN if specified
//   if (joinTable && joinCondition) {
//     const joinData = await readCSV(`${joinTable}.csv`);
//     data = data.flatMap((mainRow) => {
//       return joinData
//         .filter((joinRow) => {
//           const mainValue = mainRow[joinCondition.left.split(".")[1]];
//           const joinValue = joinRow[joinCondition.right.split(".")[1]];
//           return mainValue === joinValue;
//         })
//         .map((joinRow) => {
//           return fields.reduce((acc, field) => {
//             const [tableName, fieldName] = field.split(".");
//             acc[field] =
//               tableName === table ? mainRow[fieldName] : joinRow[fieldName];
//             return acc;
//           }, {});
//         });
//     });
//   }

//   //7TH STEP
//   // Apply WHERE clause filtering
//   // const filteredData = whereClauses.length > 0
//   //     ? data.filter(row => whereClauses.every(clause => {
//   //         // You can expand this to handle different operators
//   //         return row[clause.field] === clause.value;
//   //     }))
//   //     : data;
//   //

//   const filteredData =
//     whereClauses.length > 0
//       ? data.filter((row) =>
//           whereClauses.every((clause) => evaluateCondition(row, clause))
//         )
//       : data;

//   // Select the specified fields
//   return filteredData.map((row) => {
//     const selectedRow = {};
//     fields.forEach((field) => {
//       selectedRow[field] = row[field];
//     });
//     return selectedRow;
//   });
// }
// async function executeSELECTQuery(query) {
//     const { fields, table } = parseQuery(query);
//     const data = await readCSV(`${table}.csv`);
    
//     // Filter the fields based on the query
//     return data.map(row => {
//         const filteredRow = {};
//         fields.forEach(field => {
//             filteredRow[field] = row[field];
//         });
//         return filteredRow;
//     });
// }

//step5
// src/index.js

// const parseQuery = require('./queryParser');
// const readCSV = require('./csvReader');

// async function executeSELECTQuery(query) {
//     const { fields, table, whereClause } = parseQuery(query);
//     const data = await readCSV(`${table}.csv`);
    
//     // Filtering based on WHERE clause
//     const filteredData = whereClause
//         ? data.filter(row => {
//             const [field, value] = whereClause.split('=').map(s => s.trim());
//             return row[field] === value;
//         })
//         : data;

//     // Selecting the specified fields
//     return filteredData.map(row => {
//         const selectedRow = {};
//         fields.forEach(field => {
//             selectedRow[field] = row[field];
//         });
//         return selectedRow;
//     });
// }

// module.exports = executeSELECTQuery;



// // src/index.js
// function evaluateCondition(row, clause) {
//   const { field, operator, value } = clause;
//   switch (operator) {
//     case "=":
//       return row[field] === value;
//     case "!=":
//       return row[field] !== value;
//     case ">":
//       return row[field] > value;
//     case "<":
//       return row[field] < value;
//     case ">=":
//       return row[field] >= value;
//     case "<=":
//       return row[field] <= value;
//     default:
//       throw new Error(`Unsupported operator: ${operator}`);
//   }
// }

// // module.exports = executeSELECTQuery;

//step6
// src/index.js

// const parseQuery = require('./queryParser');
// const readCSV = require('./csvReader');

// async function executeSELECTQuery(query) {
//     const { fields, table, whereClauses } = parseQuery(query);
//     const data = await readCSV(`${table}.csv`);

//     // Apply WHERE clause filtering
//     const filteredData = whereClauses.length > 0
//         ? data.filter(row => whereClauses.every(clause => {
//             // You can expand this to handle different operators
//             return row[clause.field] === clause.value;
//         }))
//         : data;

//     // Select the specified fields
//     return filteredData.map(row => {
//         const selectedRow = {};
//         fields.forEach(field => {
//             selectedRow[field] = row[field];
//         });
//         return selectedRow;
//     });
// }

// module.exports = executeSELECTQuery;

//step7
// src/index.js

// const parseQuery = require('./queryParser');
// const readCSV = require('./csvReader');

// async function executeSELECTQuery(query) {
//     const { fields, table, whereClauses } = parseQuery(query);
//     const data = await readCSV(`${table}.csv`);

//     // Apply WHERE clause filtering
//     const filteredData = whereClauses.length > 0
//     ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
//     : data;

//     // Select the specified fields
//     return filteredData.map(row => {
//         const selectedRow = {};
//         fields.forEach(field => {
//             selectedRow[field] = row[field];
//         });
//         return selectedRow;
//     });
// }

// function evaluateCondition(row, clause) {
//     const { field, operator, value } = clause;
//     switch (operator) {
//         case '=': return row[field] === value;
//         case '!=': return row[field] !== value;
//         case '>': return row[field] > value;
//         case '<': return row[field] < value;
//         case '>=': return row[field] >= value;
//         case '<=': return row[field] <= value;
//         default: throw new Error(`Unsupported operator: ${operator}`);
//     }
// }
// module.exports = executeSELECTQuery;

//step8
const parseQuery = require('./queryParser');
const readCSV = require('./csvReader');

async function executeSELECTQuery(query) {
    // src/index.js at executeSELECTQuery

// Now we will have joinTable, joinCondition in the parsed query
const { fields, table, whereClauses, joinTable, joinCondition } = parseQuery(query);
let data = await readCSV(`${table}.csv`);

// Perform INNER JOIN if specified
if (joinTable && joinCondition) {
    const joinData = await readCSV(`${joinTable}.csv`);
    data = data.flatMap(mainRow => {
        return joinData
            .filter(joinRow => {
                const mainValue = mainRow[joinCondition.left.split('.')[1]];
                const joinValue = joinRow[joinCondition.right.split('.')[1]];
                return mainValue === joinValue;
            })
            .map(joinRow => {
                return fields.reduce((acc, field) => {
                    const [tableName, fieldName] = field.split('.');
                    acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
                    return acc;
                }, {});
            });
    });
}

// Apply WHERE clause filtering after JOIN (or on the original data if no join)
const filteredData = whereClauses.length > 0
    ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
    : data;

    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            // Assuming 'field' is just the column name without table prefix
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}
module.exports = executeSELECTQuery;
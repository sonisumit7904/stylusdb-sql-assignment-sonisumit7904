#!/usr/bin/env node

const readline = require("readline");
const {
  executeSELECTQuery,
  executeINSERTQuery,
  executeDELETEQuery,
} = require("./queryExecutor.js");

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

rl.setPrompt("SQL> ");
console.log(
  'SQL Query Engine CLI. Enter your SQL commands, or type "exit" to quit.'
);

rl.prompt();

rl.on("line", async (line) => {
  if (line.toLowerCase() === "exit") {
    rl.close();
    return;
  }

  try {
    // Execute the query - do your own implementation
    const query = line.trim();

    // Determine the type of SQL command (SELECT, INSERT, DELETE, etc.)
    const queryType = determineQueryType(query);

    // Execute the appropriate query based on the command type
    switch (queryType) {
      case "SELECT":
        const result = await executeSELECTQuery(query);
        console.log("Result:", result);
        break;
      case "INSERT":
        await executeINSERTQuery(query);
        console.log("Successfully Inserted");
        break;
      case "DELETE":
        const msg = await executeDELETEQuery(query);
        console.log(msg.message);
        break;
      default:
        throw new Error(`Unsupported SQL command: ${query}`);
    }
  } catch (error) {
    console.error("Error:", error.message);
  }

  rl.prompt();
}).on("close", () => {
  console.log("Exiting SQL CLI");
  process.exit(0);
});

function determineQueryType(query) {
  const normalizedQuery = query.trim().toLowerCase();
  if (normalizedQuery.startsWith("select")) {
    return "SELECT";
  } else if (normalizedQuery.startsWith("insert")) {
    return "INSERT";
  } else if (normalizedQuery.startsWith("delete")) {
    return "DELETE";
  } else {
    throw new Error(`Unsupported SQL command: ${query}`);
  }
}

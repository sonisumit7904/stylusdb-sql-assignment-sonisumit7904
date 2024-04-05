// src/csvReader.js

const fs = require("fs");
const csv = require("csv-parser");

function readCSV(filePath) {
  const results = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (data) => results.push(data))
      .on("end", () => {
        resolve(results);
      })
      .on("error", (error) => {
        reject(error);
      });
  });
}

async function writeCSV(filename, data) {
  if (data.length < 1) {
    throw new Error("No data to write to CSV");
  }

  // Prepare headers for CSV file
  const fields = Object.keys(data[0]);
  // let orderedFields = fields;
  // let existingData = [];

  // if (fs.existsSync(filename)) {
  //   // Read existing data from CSV file
  //   existingData = await readCSV(filename);
  //   orderedFields =
  //     existingData.length >= 1 ? Object.keys(existingData[0]) : fields;
  //   console.log(existingData);
  //}
  const outputStream = fs.createWriteStream(filename);

  const headers = fields.join(",") + "\n";
  outputStream.write(headers);

  // Prepare new rows to write to CSV file
  const newDataRows = data.map((row) => {
    return fields.map((field) => row[field]).join(",");
  });

  // Combine existing data with new data
  // const allRows = existingData
  //   .map((row) => {
  //     return orderedFields.map((field) => row[field]).join(",");
  //   })
  //   .concat(newDataRows);

  // Write all rows to CSV file
  newDataRows.forEach((rowData) => {
    outputStream.write(rowData + "\n");
  });

  outputStream.end();

  return new Promise((resolve, reject) => {
    outputStream.on("finish", () => {
      resolve();
    });

    outputStream.on("error", (err) => {
      console.error(
        `Error occurred while writing CSV file "${filename}":`,
        err
      );
      reject(err);
    });
  });
}

module.exports = { readCSV, writeCSV };

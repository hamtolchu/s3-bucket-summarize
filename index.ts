import { spawn } from 'child_process';
import { writeFile } from 'fs/promises';
import {
  pipe,
  map,
  toArray,
  join,
  split,
  filter,
  isEmpty,
  nth,
  last,
  size,
  sum,
  toAsync,
  fromEntries,
  entries,
  concurrent,
} from '@fxts/core';
import bytes from 'bytes';

interface SummaryHeaderRow {
  type: string;
  label: string;
  value: number;
}

interface SummaryContentRow {
  type: 'rows';
  date: string;
  time: string;
  size: number;
  fileName: string;
}

type SummaryRow = SummaryHeaderRow | SummaryContentRow;

type ResultTuple = [
  string,
  {
    count: number;
    size: number;
    size_mb: string;
    avg: number;
    avg_mb: string;
  },
];

const BUCKET_NAME = process.env.BUCKET_NAME || '';

const isNotEmpty = (value: unknown) => !isEmpty(value);

const parsePrefixList = (lines: string[]) => JSON.parse(lines.join('').replace(/\n/g, '')) as string[];
const extractPrefixList = () =>
  new Promise((resolve, reject) => {
    const listCommand = spawn('./scripts/runExtractPrefixList.sh', [BUCKET_NAME]);
    const lines: string[] = [];
    listCommand.stdout.setEncoding('utf8');
    listCommand.stdout.on('data', (data) => {
      lines.push(data.toString());
    });
    listCommand.stderr.on('data', (data) => {
      reject(data);
    });
    listCommand.on('close', (code) => {
      resolve(parsePrefixList(lines));
      console.log(`child process exited with code : ${code}`);
    });
  });

const checkTotalObjectRow = (line: string) => line.startsWith('Total Objects');
const checkTotalSizeRow = (line: string) => line.startsWith('Total Size');

const parseHeaderRow = (line: string, label: string): SummaryHeaderRow => {
  const [_, data] = line.split(':');
  return {
    type: label,
    label,
    value: parseInt(data.trim(), 10),
  };
};

const parseContentRow = (line: string): SummaryContentRow => {
  const [date, time, size, fileName] = pipe(line.split(/\s/i), filter(isNotEmpty), toArray);
  return {
    type: 'rows',
    date,
    time,
    size: parseInt(size, 10),
    fileName,
  };
};

const parsePrefixSummaryLine = (line: string): SummaryRow => {
  const isTotalObjectRow = checkTotalObjectRow(line);
  const isTotalSizeRow = checkTotalSizeRow(line);
  if (isTotalObjectRow) {
    return parseHeaderRow(line, 'total_object');
  }
  if (isTotalSizeRow) {
    return parseHeaderRow(line, 'total_size');
  }
  return parseContentRow(line);
};

const parsePrefixSummary = (lines: string[]) =>
  pipe(
    lines,
    join(''),
    split('\n'),
    filter(isNotEmpty),
    map((line) => line.trim()),
    map(parsePrefixSummaryLine),
    toArray,
  );

const extractPrefixSummary = (prefix: string) =>
  new Promise((resolve, reject) => {
    const listCommand = spawn('./scripts/runPrefixSummary.sh', [BUCKET_NAME, prefix]);
    const lines: string[] = [];
    listCommand.stdout.setEncoding('utf8');
    listCommand.stdout.on('data', (data) => {
      lines.push(data.toString());
    });
    listCommand.stderr.on('data', (data) => {
      reject(data);
    });
    listCommand.on('close', (code) => {
      resolve(parsePrefixSummary(lines));
      console.log(`child process exited with code : ${code}`);
    });
  });

const getTotalObjectsFromSummary = (summaries: SummaryRow[]) => {
  const length = size(summaries);
  return (nth(length - 2, summaries) as SummaryHeaderRow).value;
};

const getTotalSizeFromSummary = (summaries: SummaryRow[]) => {
  return (last(summaries) as SummaryHeaderRow).value;
};

const bytesToMegabyte = (value: number) =>
  bytes(value, {
    unit: 'MB',
    unitSeparator: ' ',
  });

const run = async () => {
  const list = await extractPrefixList();

  // NOTE: 전체 리스트 데이터 백업
  await writeFile('FULL.json', JSON.stringify(list));

  const bucketSummaryList = await pipe(
    list as string[],
    toAsync,
    map(async (prefix) => {
      const summaries = (await extractPrefixSummary(prefix)) as SummaryRow[];
      const count = getTotalObjectsFromSummary(summaries);
      const size = getTotalSizeFromSummary(summaries);
      const date = pipe(prefix.split(/\//), filter(isNotEmpty), last);
      const avg = Math.floor(size / count);
      return [date, { count, size, avg, size_mb: bytesToMegabyte(size), avg_mb: bytesToMegabyte(avg) }] as ResultTuple;
    }),
    concurrent(10),
    fromEntries,
  );

  const bucketTotalObjectCount = pipe(
    entries(bucketSummaryList),
    map((args) => {
      const [_, { count }] = args;
      return count;
    }),
    sum,
  );

  const bucketTotalSize = pipe(
    entries(bucketSummaryList),
    map((args) => {
      const [_, { size }] = args;
      return size;
    }),
    sum,
  );

  await writeFile('./OUTPUT.json', JSON.stringify(bucketSummaryList));
  await writeFile(
    './TOTAL.json',
    JSON.stringify({
      total_count: bucketTotalObjectCount,
      total_size: bucketTotalSize,
      total_size_mb: bytesToMegabyte(bucketTotalSize),
    }),
  );
};

run();

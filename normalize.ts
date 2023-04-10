// @ts-nocheck
import { pipe, map, entries, join } from '@fxts/core';
import dayjs from 'dayjs';

import { writeFile } from 'fs/promises';

import OUTPUT_LIST from './output/OUTPUT.json';

const create = (target: string) => {
  const header = 'date,count';
  const rows = pipe(
    entries(OUTPUT_LIST),
    map((args) => {
      const [key, value] = args;
      const date = dayjs(key, 'YYYYMMDD').format('YYYY-MM-DD');
      return `${date},${value[target]}`;
    }),
  );
  return pipe([header, ...rows], join('\n'));
};

const run = async () => {
  // const countVar = create('count');
  const sizeVar = create('size');
  // await writeFile('./countVar.csv', countVar);
  await writeFile('./sizeVar.csv', sizeVar);
};

run();

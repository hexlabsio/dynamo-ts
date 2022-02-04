import { cars } from '../examples/car-makes';
import { v4 as uuid } from 'uuid';
import {
  queryAllNissanModelsThatBeginWith3AndFlipOrderUsingIndex,
  queryNissansFrom2006,
  tableClient,
} from '../examples/query-filter-expressions';

describe('Dynamo Querier', () => {
  beforeAll(async () => {
    tableClient.logStatements(false);
    await Promise.all(
      cars.map(async (car) => {
        await Promise.all(
          car.colours.map(async (colour) => {
            await Promise.all(
              car.years.map(async (year) => {
                await Promise.all(
                  new Array(5).fill(0).map(async () => {
                    const identifier = uuid();
                    await tableClient.put({
                      make: car.make,
                      model: car.model,
                      colour: colour,
                      year,
                      identifier,
                    });
                  }),
                );
              }),
            );
          }),
        );
      }),
    );
    tableClient.logStatements(true);
  }, 3000);
  it('should query all Nissans from 2006', async () => {
    const result = await queryNissansFrom2006();
    expect(result.length).toEqual(10);
    const allAre2006Nissan =
      !result.some((it) => it.make !== 'Nissan') &&
      !result.some((it) => it.year !== 2006);
    expect(allAre2006Nissan).toEqual(true);
  });
  it('should query all Nissans beginning with 3', async () => {
    const result =
      await queryAllNissanModelsThatBeginWith3AndFlipOrderUsingIndex();
    expect(result.length).toEqual(40);
    const allAreNissans = !result.some((it) => it.make !== 'Nissan');
    expect(allAreNissans).toEqual(true);
    const allModelsStartWith3 = !result.some((it) => !it.model.startsWith('3'));
    expect(allModelsStartWith3).toEqual(true);
  });
});

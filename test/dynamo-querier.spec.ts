import { cars } from "../examples/car-makes";
import { v4 as uuid } from "uuid";
import {
  queryAllNissanModelsThatBeginWith3AndFlipOrderUsingIndex,
  queryNissansBetween2006And2007ThatAreMetallicBlack,
  queryNissansBetween2006And2022,
  queryNissansFrom2006,
  tableClient,
} from "../examples/query-filter-expressions";

describe("Dynamo Querier", () => {
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
                  })
                );
              })
            );
          })
        );
      })
    );
    tableClient.logStatements(true);
  }, 3000);
  it("should query all Nissans from 2006", async () => {
    const result = await queryNissansFrom2006();
    expect(result.length).toEqual(10);
    const allAre2006Nissan =
      !result.some((it) => it.make !== "Nissan") &&
      !result.some((it) => it.year !== 2006);
    expect(allAre2006Nissan).toEqual(true);
  });

  it("should query all Nissans beginning with 3", async () => {
    const result =
      await queryAllNissanModelsThatBeginWith3AndFlipOrderUsingIndex();
    expect(result.length).toEqual(40);
    const allAreNissans = !result.some((it) => it.make !== "Nissan");
    expect(allAreNissans).toEqual(true);
    const allModelsStartWith3 = !result.some((it) => !it.model.startsWith("3"));
    expect(allModelsStartWith3).toEqual(true);
  });

  it("should query all Nissans between 2006 and 2022", async () => {
    const result = await queryNissansBetween2006And2022();
    expect(result.length).toEqual(90);
    const allAreNissans = !result.some((it) => it.make !== "Nissan");
    expect(allAreNissans).toEqual(true);
    const allHaveYearsBetween2006And2022 = !result.some(
      (it) => !(it.year >= 2006 && it.year <= 2022)
    );
    expect(allHaveYearsBetween2006And2022).toEqual(true);
  });

  it("should fetch all Nissans", async () => {
    const result = await tableClient.queryAll({
      make: "Nissan",
      projection: (projector) => projector.project("model"),
      queryLimit: 1,
    });
    expect(result.member.length).toEqual(100);
  });

  it("should fetch limited number of Nissans ", async () => {
    const query = { make: "Nissan", queryLimit: 1 };
    const result1 = await tableClient.queryAll(query);
    const query2Offset = result1.next;
    const result2 = await tableClient.queryAll({
      next: query2Offset,
      ...query,
    });
    expect(result1.member.length).toEqual(1);
    expect(result2.member.length).toEqual(1);
    expect(result1.member).not.toEqual(result2.member);
  }, 50000);

  it("should fetch all Nissans up to 2006", async () => {
    const result = await tableClient.queryAll({
      make: "Nissan",
      filter: (compare) => compare().year.lte(2006),
      projection: (projector) => projector.project("model"),
    });
    expect(result.member.length).toEqual(20);
  });

  it("should retrieve all Nissans if limit greater than items in table", async () => {
    const result = await tableClient.queryAll({
      make: "Nissan",
      projection: (projector) => projector.project("model"),
      queryLimit: 500000,
    });
    expect(result.member.length).toEqual(100);
  });

  it("should limit fetching all Nissans", async () => {
    const result = await tableClient.queryAll({
      make: "Nissan",
      projection: (projector) => projector.project("model"),
      queryLimit: 50,
    });
    expect(result.member.length).toEqual(50);
  });

  it("should correctly project fetch all Nissans", async () => {
    const result = await tableClient.queryAll({
      make: "Nissan",
      projection: (projector) => projector.project("model"),
    });

    const validProjections = result.member.filter((member) => {
      const keys = Object.keys(member);
      return keys.length == 1 && keys[0] === "model";
    });

    expect(validProjections.length).toEqual(result.member.length);
  });

  it("should query all Nissans between 2006 & 2007 that are metallic black", async () => {
    const result = await queryNissansBetween2006And2007ThatAreMetallicBlack();
    expect(result.length).toEqual(10);
    const allAreNissansAndMetallicBlack = !result.some(
      (it) => it.make !== "Nissan" || it.colour !== "Metallic Black"
    );
    expect(allAreNissansAndMetallicBlack).toEqual(true);
    const allHaveYearsBetween2006And2007 = !result.some(
      (it) => !(it.year >= 2006 && it.year <= 2007)
    );
    expect(allHaveYearsBetween2006And2007).toEqual(true);
  });

  it("should query and fetch 100 Nissans", async () => {
    const result = await tableClient.queryAll({
      make: "Nissan",
      projection: (projector) => projector.project("model"),
      queryLimit: 250,
    });
    expect(result.member.length).toEqual(100);
  });

  it("should exclude enriched key fields added internally by queryAll from result", async () => {
    const result = await tableClient.queryAll({
      make: "Nissan",
      projection: (projector) => projector.project("model"),
      queryLimit: 250,
    });
    expect(result.member).toBeDefined();
    //ensure identifier that was added to projection is not present
    const keysInResultSet = result.member.some((it) => {
      const rec = it as Record<string, unknown>;
      return rec["identifier"] !== undefined || rec["make"] !== undefined;
    });
    expect(keysInResultSet).toEqual(false);
  });

  it("should queryAll Nissans between 2006 & 2007 that are metallic black", async () => {
    const result = await tableClient.queryAll({
      make: "Nissan",
      filter: (compare) =>
        compare()
          .year.between(2006, 2007)
          .and(compare().colour.eq("Metallic Black")),
      queryLimit: 6,
      dynamo: { Limit: 1 },
    });
    const resultSet = result.member;
    expect(resultSet.length).toEqual(6);
    const allAreNissansAndMetallicBlack = !resultSet.some(
      (it) => it.make !== "Nissan" || it.colour !== "Metallic Black"
    );
    expect(allAreNissansAndMetallicBlack).toEqual(true);
    const allHaveYearsBetween2006And2007 = !resultSet.some(
      (it) => !(it.year >= 2006 && it.year <= 2007)
    );
    expect(allHaveYearsBetween2006And2007).toEqual(true);
  });
});

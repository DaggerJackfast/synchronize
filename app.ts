import { Collection, MongoClient } from "mongodb";
import * as dotenv from "dotenv";
import { faker } from "@faker-js/faker";
import { ICustomer } from "./types";

const getRandomElement = <T>(list: T[]): T => {
  const index = Math.floor(Math.random() * list.length);
  return list[index];
};

const getRandomNumberInRange = (min: number, max: number): number => {
  if (min > max) {
    throw new Error(`max value must be more than min`);
  }
  return Math.trunc(Math.random() * (max + 1 - min) + min);
};

const connect = async (uri: string): Promise<MongoClient> => {
  const client = new MongoClient(uri);
  await client.connect();
  console.log("mongo is connected!");
  return client;
};

class CustomersGenerator {
  private readonly saveIntervalTime: number = 200;
  private readonly client: MongoClient;
  private readonly customersCollection: Collection<ICustomer>;
  private timer: NodeJS.Timer | number | null = null;

  constructor(client: MongoClient) {
    this.client = client;
    this.client = client;
    this.customersCollection = this.client
      .db("synchronize")
      .collection<ICustomer>("customers");
  }

  public start(): void {
    try {
      this.startPeriodicGenerate();
    } catch (error: unknown) {
      this.stopPeriodicGenerate();
      throw error;
    }
  }

  private startPeriodicGenerate(): void {
    this.timer = setInterval(() => {
      this.saveCustomers().then();
    }, this.saveIntervalTime);
  }

  private stopPeriodicGenerate(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  private async saveCustomers(): Promise<void> {
    const customersCount = getRandomNumberInRange(1, 10);
    const customers = this.generateCustomers(customersCount);

    await this.customersCollection.insertMany(customers);
    console.log("saveCount: ", customersCount);
  }

  private generateCustomers(count: number): ICustomer[] {
    const customers: ICustomer[] = [];
    for (let i = 0; i < count; i++) {
      const customer = this.generateCustomer();
      customers.push(customer);
    }
    return customers;
  }

  private generateCustomer(): ICustomer {
    const sex = getRandomElement<string>(["male", "female"]) as
      | "male"
      | "female";
    const firstName = faker.person.firstName(sex);
    const lastName = faker.person.lastName(sex);
    return {
      firstName,
      lastName,
      email: faker.internet.email({ firstName, lastName }),
      address: {
        line1: faker.location.streetAddress(),
        line2: faker.location.secondaryAddress(),
        postcode: faker.location.zipCode(),
        city: faker.location.city(),
        state: faker.location.state({ abbreviated: true }),
        country: faker.location.countryCode("alpha-2"),
      },
      createdAt: new Date(),
    };
  }
}

const main = async (): Promise<void> => {
  dotenv.config();
  const uri = <string>process.env.DB_URI;
  const client = await connect(uri);
  try {
    const generator = new CustomersGenerator(client);
    generator.start();
  } catch (error: unknown) {
    console.error(error);
    await client.close();
  }

  console.log("after connect");
};

main().then();

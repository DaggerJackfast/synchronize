import * as hash from "custom-hash";
import * as dotenv from "dotenv";
import {
  Collection,
  MongoClient,
  ObjectId,
  Document,
  FindCursor,
  UpdateOneModel,
  ResumeToken,
} from "mongodb";
import {
  CustomerChangeStreamEvent,
  CustomersChangeWatchStream,
  ICustomer,
  ICustomerDocument,
  IResumeToken,
  CanBeUndefined,
} from "./types";

const genCharArray = (firstSymbol: string, lastSymbol: string): string[] => {
  const firstCode: number = firstSymbol.charCodeAt(0);
  const lastCode: number = lastSymbol.charCodeAt(0);
  const symbols: string[] = [];
  for (let i = firstCode; i <= lastCode; i++) {
    symbols.push(String.fromCharCode(i));
  }
  return symbols;
};

const generateCharSet = (): string[] => {
  const charSet: string[] = [];
  charSet.push(...genCharArray("a", "z"));
  charSet.push(...genCharArray("A", "Z"));
  charSet.push(...genCharArray("0", "9"));
  return charSet;
};

hash.configure({ charSet: generateCharSet(), maxLength: 8 });

const generateHash = (input: string): string => hash.digest(input);

const anonymize = (customer: ICustomer): ICustomer => {
  return {
    firstName: generateHash(customer.firstName),
    lastName: generateHash(customer.lastName),
    email: anonymizeEmail(customer.email),
    address: {
      line1: generateHash(customer.address.line1),
      line2: generateHash(customer.address.line2),
      postcode: generateHash(customer.address.postcode),
      city: customer.address.city,
      country: customer.address.country,
      state: customer.address.state,
    },
    createdAt: customer.createdAt,
  };
};

const anonymizeEmail = (email: string): string => {
  const [before, domain] = email.split("@");
  return [generateHash(before), domain].join("@");
};

type UpdateOneOperation<T extends Document = Document> = {
  updateOne: UpdateOneModel<T>;
};

class CustomersAnonymizer {
  private readonly maxWriteBatchSize: number = 100000;
  private readonly batchCount: number = 1000;
  private readonly saveIntervalTime: number = 1000;
  private readonly client: MongoClient;
  private customerDocuments: ICustomerDocument[] = [];
  private timer: CanBeUndefined<number | NodeJS.Timer> = undefined;
  private resumeToken: CanBeUndefined<ResumeToken> = undefined;
  private readonly anonymisedCollection: Collection<ICustomerDocument>;
  private readonly customersCollection: Collection<ICustomerDocument>;
  private readonly resumeTokenCollection: Collection<IResumeToken>;

  constructor(client: MongoClient) {
    this.client = client;
    this.anonymisedCollection = this.client
      .db("synchronize")
      .collection<ICustomerDocument>("customers_anonymised");
    this.customersCollection = this.client
      .db("synchronize")
      .collection<ICustomerDocument>("customers");
    this.resumeTokenCollection = this.client
      .db("synchronize")
      .collection<IResumeToken>("customers_resume_token");
  }

  public async start(fullReindex?: boolean): Promise<void> {
    if (fullReindex) {
      await this.fullReindex();
      process.exit(0);
    }
    await this.loadSavedResumeToken();
    await this.watch();
  }

  private async fullReindex(): Promise<void> {
    const customerCursor: FindCursor<ICustomerDocument> =
      await this.customersCollection.find();
    await this.saveWithCursor(customerCursor);
  }

  private async loadSavedResumeToken(): Promise<void> {
    const resume = await this.resumeTokenCollection.findOne(
      { id: "resumeToken" },
      { projection: { resumeToken: 1 }, limit: 1 }
    );
    if (!resume) {
      return;
    }
    this.resumeToken = resume.resumeToken;
  }

  private async saveResumeToken(): Promise<void> {
    if (!this.resumeToken) {
      return;
    }
    const resumeToken = this.resumeToken;
    await this.resumeTokenCollection.updateOne(
      { where: { id: "resumeToken" } },
      { $set: { id: "resumeToken", resumeToken } },
      { upsert: true }
    );
  }

  private async saveWithCursor(
    customerCursor: FindCursor<ICustomerDocument>
  ): Promise<void> {
    let updateDocuments: ICustomerDocument[] = [];
    while (await customerCursor.hasNext()) {
      const document = (await customerCursor.next()) as ICustomerDocument;
      updateDocuments.push(document);
      if (updateDocuments.length >= this.maxWriteBatchSize) {
        await this.save(updateDocuments);
        updateDocuments = [];
      }
    }
    if (updateDocuments.length > 0) {
      await this.save(updateDocuments);
    }
  }

  private async watch(): Promise<void> {
    try {
      this.startIntervalSave();
      const pipeline = [
        {
          $match: {
            operationType: { $in: ["update", "insert"] },
          },
        },
      ];
      const changeStream: CustomersChangeWatchStream =
        this.customersCollection.watch(pipeline, {
          fullDocument: "updateLookup",
          resumeAfter: this.resumeToken,
        });

      changeStream.on("change", (event: CustomerChangeStreamEvent): void => {
        if (!event.fullDocument) {
          console.log("document does not exists");
          return;
        }
        const document: ICustomerDocument = event.fullDocument;
        this.resumeToken = event._id as string;
        this.customerDocuments.push(document);
        if (this.customerDocuments.length >= this.batchCount) {
          this.saveResumeToken();
          const customers = this.customerDocuments;
          this.customerDocuments = [];
          this.save(customers);
        }
      });
    } catch (error: unknown) {
      console.error(error);
      this.stopIntervalSave();
      throw error;
    }
  }

  private startIntervalSave(): void {
    this.timer = setInterval(() => {
      this.saveResumeToken();
      const customers = this.customerDocuments;
      this.save(customers);
      this.customerDocuments = [];
    }, this.saveIntervalTime);
  }

  private stopIntervalSave(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = undefined;
    }
  }

  private createCustomer(document: ICustomerDocument): ICustomer {
    return {
      firstName: document.firstName,
      lastName: document.lastName,
      email: document.email,
      address: {
        line1: document.address.line1,
        line2: document.address.line2,
        postcode: document.address.postcode,
        city: document.address.city,
        state: document.address.state,
        country: document.address.country,
      },
      createdAt: new Date(document.createdAt),
    };
  }

  public async save(documents: ICustomerDocument[]): Promise<void> {
    if (documents.length > 0) {
      console.log("save count: ", documents.length);
      const bulkOperations: UpdateOneOperation<ICustomerDocument>[] =
        documents.map((document) => {
          const customer = this.createCustomer(document);
          const anonymized = anonymize(customer);
          const bulkDocument: ICustomerDocument = {
            ...anonymized,
            _id: new ObjectId(document._id),
          };
          return {
            updateOne: {
              filter: { _id: bulkDocument._id },
              update: {
                $set: { ...bulkDocument },
              },
              upsert: true,
            },
          };
        });
      await this.anonymisedCollection.bulkWrite(bulkOperations);
    }
  }
}

const connect = async (uri: string): Promise<MongoClient> => {
  const client = new MongoClient(uri);
  await client.connect();
  return client;
};

const main = async (): Promise<void> => {
  const fullReindex = process.argv.includes("--full-reindex");
  dotenv.config();
  const uri = <string>process.env.DB_URI;
  const client = await connect(uri);
  try {
    const watcher = new CustomersAnonymizer(client);
    await watcher.start(fullReindex);
    console.log("after start watching");
  } catch (error) {
    console.error(error);
    if (client) await client.close(true);
  }
};
main().then();

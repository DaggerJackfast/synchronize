import { ChangeStream, ChangeStreamInsertDocument, ChangeStreamUpdateDocument, Document, ObjectId } from "mongodb";
export type CollectionChangeWatchStream<T extends Document = Document> = ChangeStream<
  T,
  ChangeStreamInsertDocument<T> | ChangeStreamUpdateDocument<T>
>;
export type ChangeStreamEvent<T extends Document = Document> = ChangeStreamInsertDocument<T> | ChangeStreamUpdateDocument<T>;

export interface IAddress {
  line1: string;
  line2: string;
  postcode: string;
  city: string;
  state: string;
  country: string;
}

export interface ICustomer {
  firstName: string;
  lastName: string;
  email: string;
  address: IAddress;
  createdAt: Date;
}

export interface ICustomerDocument extends ICustomer {
  _id: ObjectId | string;
}
import {
  ChangeStream,
  ChangeStreamInsertDocument,
  ChangeStreamUpdateDocument,
  ObjectId,
  ResumeToken,
} from "mongodb";

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

export interface IResumeToken {
  id: "resumeToken";
  resumeToken: ResumeToken;
}

export type CanBeUndefined<T> = undefined | T;

export type CustomersChangeWatchStream = ChangeStream<
  ICustomerDocument,
  | ChangeStreamInsertDocument<ICustomerDocument>
  | ChangeStreamUpdateDocument<ICustomerDocument>
>;

export type CustomerChangeStreamEvent =
  | ChangeStreamInsertDocument<ICustomerDocument>
  | ChangeStreamUpdateDocument<ICustomerDocument>;

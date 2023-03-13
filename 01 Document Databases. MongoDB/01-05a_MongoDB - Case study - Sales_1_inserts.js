//===============================================================================
//                                      Case study:  SALES
//===============================================================================
// last update: 2022-12-06

//--   show databases on the server
show dbs

//-- show current database
db

//--   select current database
use sales

//--   list all colections in current database
show collections


//==================================================================================
//                              first collection - "counties"
db.counties.remove({}) ;
db.counties.insertOne ( { _id : 'IS', countyName : 'Iasi', countyRegion  : 'Moldova' });
db.counties.insertOne ( { _id : 'B', countyName : 'Bucuresti'});
db.counties.insertOne ( { _id : 'VN', countyName : 'Vrancea', countyRegion  : 'Moldova' });
db.counties.insertOne ( { _id : 'NT', countyName : 'Neamt', countyRegion  : 'Moldova' });
db.counties.insertOne ( { _id : 'SV', countyName : 'Suceava', countyRegion  : 'Moldova' });
db.counties.insertOne ( { _id : 'VS', countyName : 'Vaslui', countyRegion  : 'Moldova' });
db.counties.insertOne ( { _id : 'TM', countyName : 'Timis', countyRegion  : 'Banat'});
db.counties.insertOne ( { _id : 'DJ', countyName : 'Dolj', countyRegion  : 'Oltenia'});

// indexes
db.counties.createIndex({_id : 1}) ;
db.counties.createIndex({countyName: 1}, {unique: true}) ;
db.counties.createIndex({countyRegion : 1}, {unique: false}) ;

db.counties.find() ;




//==================================================================================
//                      second collection - "postalCodes"
db.postalCodes.remove({}) ;
db.postalCodes.insertOne ( { _id : '700505', cityName : 'Iasi', countyCode : 'IS' });
db.postalCodes.insertOne ( { _id : '701150', cityName : 'Pascani', countyCode : 'IS' });
db.postalCodes.insertOne ( { _id : '706500', cityName : 'Vaslui', countyCode : 'VS' });
db.postalCodes.insertOne ( { _id : '705300', cityName : 'Focsani', countyCode : 'VN' });
db.postalCodes.insertOne ( { _id : '706400', cityName : 'Birlad', countyCode : 'VS' });
db.postalCodes.insertOne ( { _id : '705800', cityName : 'Suceava', countyCode : 'SV' });
db.postalCodes.insertOne ( { _id : '705550', cityName : 'Roman', countyCode : 'NT' });
db.postalCodes.insertOne ( { _id : '701900', cityName : 'Timisoara', countyCode : 'TM' });

// indexes
db.postalCodes.createIndex({_id : 1}) ;
db.postalCodes.createIndex({cityName: 1}, {unique: false}) ;
db.postalCodes.createIndex({countyCode: 1}, {unique: false}) ;

db.postalCodes.find() ;


// Question: What do you think about this structure (do not execute the command!!!) ?
db.counties2.insertOne ( { _id : 'IS', countyName : 'Iasi', countyRegion  : 'Moldova' ,
	zip_codes : [
		{ post_code : '700505', cityName : 'Iasi' },
		{ post_code : '701150', cityName : 'Pascani' }
	]
});


// Question: ... and how about this structure (do not execute the command!!!) ?
db.postalCodes2.insertOne ( {
	_id : '700505',
	cityName : 'Iasi',
	county : { countyCode : 'IS', countyName : 'Iasi', countyRegion  : 'Moldova' }
 });




//==================================================================================
//          					third collection - customers
//   each document will be identified by system object id

db.customers.remove({}) ;

db.customers.insertOne ( {
	custName : 'Client 1 SRL',
	custFiscalCode: 'R1001',
	address : 'Tranzitiei, 13 bis',
	postCode : '700505',
	contacts : [
			{ person : {
						persCode : 'CNP1',
						familyName : 'Ioan',
						surName : 'Vasile',
						address : 'I.L.Caragiale, 22',
						sex : 'B',
						postCode : '700505',
						homePhone : '0232123456',
						officePhone : '0232987654',
						mobilePhone : '0742222222'
								},
				position :  'Director general'
			}
		] }) ;

db.customers.insertOne ( { custName : 'Client 2 SA', custFiscalCode: 'R1002', postCode : '700505', phone : '0232212121',
	contacts : [
		{ person : { persCode : 'CNP2', familyName : 'Vasile', surName : 'Ion',
			sex : 'B', postCode : '700505', homePhone : '0234234567',
			officePhone : '0234876543', mobilePhone : '0794222223', email : 'Ion@a.ro'
					}, position :  'Director general' },
		{ person : { persCode : 'CNP3', familyName : 'Popovici', surName : 'Ioana',
			address : 'V.Micle, Bl.I, Sc.B,Ap.2', sex : 'F', postCode : '701150',
			homePhone : '0233534568', mobilePhone : '0744222224'
					}, position :  'Sef aprovizionare' }
		] }) ;

db.customers.insertOne ( { custName : 'Client 3 SRL', custFiscalCode: 'R1003', address : 'Prosperitatii, 22',
	postCode : '706500', phone : '0235222222',
	contacts : [
		{ person : { persCode : 'CNP4', familyName : 'Lazar', surName : 'Caraion',
			address : 'M.Eminescu, 42', sex : 'B', postCode : '706500', homePhone : '0233534568',
			officePhone : '0235222225',
					}, position :  'Director general' },
		{ person : { persCode : 'CNP5', familyName : 'Iurea', surName : 'Simion',
			address : 'I.Creanga, 44 bis', sex : 'B', postCode : '706500',
			homePhone : '023567890', mobilePhone : '0235543210'
					}, position :  'Director financiar' }
		] }) ;

db.customers.insertOne ( { custName : 'Client 4', address : 'Sapientei, 56',
	contacts : [ 	{ person : { persCode : 'CNP6', familyName : 'Vasc', surName : 'Simona',
			address : 'M.Eminescu, 13', sex : 'F', postCode : '701150',
			officePhone : '0237432109', mobilePhone : '0794222227'
					}, position :  'Director general' } ] }) ;

db.customers.insertOne ( { custName : 'Client 5 SRL', custFiscalCode: 'R1005',
	postCode : '701900', phone :  '0567111111',
	contacts : [ 	{ person : { persCode : 'CNP7', familyName : 'Popa', surName : 'Ioanid',
			address : 'I.Ion, Bl.H2, Sc.C, Ap.45', sex : 'B', postCode : '701900',
			homesPhone : '0238789012', officePhone : '0238321098'
					}, position :  'Sef aprovizionare' } ] }) ;

db.customers.insertOne ( { custName : 'Client 6 SA', custFiscalCode: 'R1006', address : 'Pacientei, 33',
	postCode : '705550',
	contacts : [ 	{ person : { persCode : 'CNP8', familyName : 'Bogacs', surName : 'Ildiko',
			address : 'I.V.Viteazu, 67', sex : 'F', postCode : '705550',
			homePhone : '0239890123', officePhone : '0239210987', mobilePhone : '0722222299'
					}, position :  'Director financiar' } ] }) ;

db.customers.insertOne ( { custName : 'Client 7 SRL', custFiscalCode: 'R1007', address :  'Victoria Capitalismului, 2',
	postCode : '701900', phone : '0567121212',
	contacts : [ 	{ person : { persCode : 'CNP9', familyName : 'Ioan', surName : 'Vasilica',
			address : 'Garii, Bl.B4, Sc.A, Ap.1', sex : 'F', postCode : '701900',
			homePhone : '0240901234', officePhone : '0240109876', mobilePhone : '0779422223'
					}, position :  'Sef aprovizionare' } ] }) ;


db.customers.update ( {custName : "Client 4"},  {$addToSet : { contacts : { $each : [
		{ person :
			{ persCode : 'CNP7', familyName : 'Popa', surName : 'Ioanid',
			address : 'I.Ion, Bl.H2, Sc.C, Ap.45', sex : 'B', postCode : '701900',
			homesPhone : '0238789012', officePhone : '0238321098'},
			  position :  'Consultant aprovizionare' },
		{ person :
			{ persCode : 'CNP8', familyName : 'Bogacs', surName : 'Ildiko',
			address : 'I.V.Viteazu, 67', sex : 'F', postCode : '705550',
			homePhone : '0239890123', officePhone : '0239210987', mobilePhone : '0722222299'},
			  position :  'Consultant financiar' } ] }}}) ;


db.customers.find().pretty() ;


//==================================================================================

//         		fourth collection - products
// 	example of upsert  (update combined with insertOne)

db.products.remove({}) ;
db.products.update ( {_id : 1},
	{_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Cosmetice',  percVAT  : .24 },
		{upsert : true} ) ;
db.products.update ( {_id : 2},
	{_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		{upsert : true} ) ;
db.products.update ( {_id : 3},
	{_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		{upsert : true} ) ;
db.products.update ( {_id : 4},
	{_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		{upsert : true} ) ;
db.products.update ( {_id : 5},
	{_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Cosmetice',  percVAT  : .24 },
		{upsert : true} ) ;
db.products.update ( {_id : 6},
	{_id: 6, prodName : 'Produs 6', mu : 'p250g', prodCateg : 'Cafea',  percVAT  : .24 },
			{upsert : true} ) ;


db.products.find().pretty() ;


//==================================================================================
//
//											fifth collection: invoices
//
db.invoices.remove({}) ;

// invoice 1111
db.invoices.insertOne ( {_id : 1111, invDate : new ISODate("2019-08-01T11:00:00Z"),
	custID :  db.customers.findOne({ custName : 'Client 1 SRL'})._id,
	items : [
		{ line : 1,
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc',
		  	prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 50,
		  unitPrice : 1000 },
		{ line : 2,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg',
		  	prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 75,
		  unitPrice : 1050 },
		{ line : 3,
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc',
		  	prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 500,
		  unitPrice : 7060 }  ] }) ;


// invoice 1112
var myClient = db.customers.findOne ( { custName : 'Client 5 SRL'}) ;
db.invoices.insertOne ( {_id : 1112, invDate : new ISODate("2019-08-01T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 80,
		  unitPrice : 1030 },
		{ line : 2,
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 40,
		  unitPrice : 750 }  ],
		  notes : 'Probleme cu transportul' }) ;


// invoice 1113
var myClient = db.customers.findOne ( { custName : 'Client 2 SA'}) ;
db.invoices.insertOne ( {_id : 1113, invDate : new ISODate("2019-08-01T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 100,
		  unitPrice : 975 } ] }) ;


// invoice 1114
var myClient = db.customers.findOne ( { custName : 'Client 6 SA'}) ;
db.invoices.insertOne ( {_id : 1114, invDate : new ISODate("2019-08-01T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 70,
		  unitPrice : 1070 },
		{ line : 2,
		  product : {_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		  quantity : 30,
		  unitPrice : 1705 },
		{ line : 3,
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 700,
		  unitPrice : 7064 }  ] }) ;


// invoice 1115
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 1115, invDate : new ISODate("2019-08-02T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 150,
		  unitPrice : 925 } ] } ) ;


// invoice 1116
var myClient = db.customers.findOne ( { custName : 'Client 7 SRL'}) ;
db.invoices.insertOne ( {_id : 1116, invDate : new ISODate("2019-08-02T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 125,
		  unitPrice : 930 } ],
	notes : 'Pretul propus initial a fost modificat' }) ;


// invoice 1117
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 1117, invDate : new ISODate("2019-08-03T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 100,
		  unitPrice : 1000 },
		{ line : 2,
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 100,
		  unitPrice : 950 }  ] } ) ;


// invoice 1118
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 1118, invDate : new ISODate("2019-08-04T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 30,
		  unitPrice : 1100 },
		{ line : 2,
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 150,
		  unitPrice : 930 }  ] }) ;


// invoice 1119
var myClient = db.customers.findOne ( { custName : 'Client 3 SRL'}) ;
db.invoices.insertOne ( {_id : 1119, invDate : new ISODate("2019-08-07T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 35,
		  unitPrice : 1090 },
		{ line : 2,
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 40,
		  unitPrice : 700 },
		{ line : 3,
		  product : {_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		  quantity : 50,
		  unitPrice : 1410 },
		{ line : 4,
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 750,
		  unitPrice : 6300 }  ] }) ;


// invoice 1120
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 1120, invDate : new ISODate("2019-08-07T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 80,
		  unitPrice : 1120 } ] }) ;


// invoice 1121
var myClient = db.customers.findOne ( { custName : 'Client 4'}) ;
db.invoices.insertOne ( {_id : 1121, invDate : new ISODate("2019-08-07T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 550,
		  unitPrice : 7064 },
		{ line : 2,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 100,
		  unitPrice : 1050 }  ] }) ;


// invoice 1122  - empty (no lines)
var myClient = db.customers.findOne ( { custName : 'Client 5 SRL'}) ;
db.invoices.insertOne ( {_id : 1122, invDate : new ISODate("2019-08-07T11:00:00Z"), custID : myClient._id }) ;


// invoice 2111
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 2111, invDate : new ISODate("2019-08-14T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 57,
		  unitPrice : 1000 },
		{ line : 2,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 79,
		  unitPrice : 1050 },
		{ line : 3,
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 510,
		  unitPrice : 7060 }  ] }) ;


// invoice 2112
var myClient = db.customers.findOne ( { custName : 'Client 5 SRL'}) ;
db.invoices.insertOne ( {_id : 2112, invDate : new ISODate("2019-08-14T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 85,
		  unitPrice : 1030 },
		{ line : 2,
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 65,
		  unitPrice : 750 }  ],
		  notes : 'Probleme cu transportul' }) ;


// invoice 2113
var myClient = db.customers.findOne ( { custName : 'Client 2 SA'}) ;
db.invoices.insertOne ( {_id : 2113, invDate : new ISODate("2019-08-14T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 120,
		  unitPrice : 975 } ] }) ;


// invoice 2115
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 2115, invDate : new ISODate("2019-08-15T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 110,
		  unitPrice : 925 } ] } ) ;


// invoice 2116
var myClient = db.customers.findOne ( { custName : 'Client 7 SRL'}) ;
db.invoices.insertOne ( {_id : 2116, invDate : new ISODate("2019-08-15T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 135,
		  unitPrice : 930 } ],
	notes : 'Pretul propus initial a fost modificat' }) ;


// invoice 2117
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL' }) ;
db.invoices.insertOne ( {_id : 2117, invDate : new ISODate("2019-08-16T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 150,
		  unitPrice : 1000 },
		{ line : 2,
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 110,
		  unitPrice : 950 }  ] }) ;


// invoice 2118
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 2118, invDate : new ISODate("2019-08-16T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 39,
		  unitPrice : 1100 },
		{ line : 2,
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 120,
		  unitPrice : 930 }  ] }) ;


// invoice 2119
var myClient = db.customers.findOne ( { custName : 'Client 3 SRL'}) ;
db.invoices.insertOne ( {_id : 2119, invDate : new ISODate("2019-08-21T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 35,
		  unitPrice : 1090 },
		{ line : 2,
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 40,
		  unitPrice : 700 },
		{ line : 3,
		  product : {_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		  quantity : 55,
		  unitPrice : 1410 },
		{ line : 4,
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 755,
		  unitPrice : 6300 }  ] }) ;


// invoice 2121
var myClient = db.customers.findOne ( { custName : 'Client 4'}) ;
db.invoices.insertOne ( {_id : 2121, invDate : new ISODate("2019-08-21T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 550,
		  unitPrice : 7064 },
		{ line : 2,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 103,
		  unitPrice : 1050 }  ] }) ;


// invoice 2122  - empty (no lines)
var myClient = db.customers.findOne ( { custName : 'Client 5 SRL'}) ;
db.invoices.insertOne ( {_id : 2122, invDate : new ISODate("2019-08-22T11:00:00Z"), custID : myClient._id }) ;


// invoice 3111
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 3111, invDate : new ISODate("2019-09-01T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 57,
		  unitPrice : 1000 },
		{ line : 2,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 79,
		  unitPrice : 1050 },
		{ line : 3,
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 510,
		  unitPrice : 7060 }  ] }) ;


// invoice 3112
var myClient = db.customers.findOne ( { custName : 'Client 5 SRL'}) ;
db.invoices.insertOne ( {_id : 3112, invDate : new ISODate("2019-09-01T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 85,
		  unitPrice : 1030 },
		{ line : 2,
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 65,
		  unitPrice : 750 }  ],
		  notes : 'Probleme cu transportul' }) ;


// invoice 3113
var myClient = db.customers.findOne ( { custName : 'Client 2 SA'}) ;
db.invoices.insertOne ( {_id : 3113, invDate : new ISODate("2019-09-02T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 120,
		  unitPrice : 975 } ] }) ;


// invoice 3115
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 3115, invDate : new ISODate("2019-09-02T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 110,
		  unitPrice : 925 } ] } ) ;


// invoice 3116
var myClient = db.customers.findOne ( { custName : 'Client 7 SRL'}) ;
db.invoices.insertOne ( {_id : 3116, invDate : new ISODate("2019-09-10T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 135,
		  unitPrice : 930 } ],
	notes : 'Pretul propus initial a fost modificat' }) ;


// invoice 3117
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL' }) ;
db.invoices.insertOne ( {_id : 3117, invDate : new ISODate("2019-09-10T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 150,
		  unitPrice : 1000 },
		{ line : 2,
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 110,
		  unitPrice : 950 }  ] }) ;


// invoice 3118
var myClient = db.customers.findOne ( { custName : 'Client 1 SRL'}) ;
db.invoices.insertOne ( {_id : 3118, invDate : new ISODate("2019-09-17T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 39,
		  unitPrice : 1100 },
		{ line : 2,
		  product : {_id: 1, prodName : 'Produs 1', mu : 'buc', prodCateg : 'Tigari',  percVAT  : 0.24 },
		  quantity : 120,
		  unitPrice : 930 }  ] }) ;


// invoice 3119
var myClient = db.customers.findOne ( { custName : 'Client 3 SRL'}) ;
db.invoices.insertOne ( {_id : 3119, invDate : new ISODate("2019-10-07T11:00:00Z"), custID : myClient._id,
	items : [
		{ line : 1,
		  product : {_id: 2, prodName : 'Produs 2', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.12 },
		  quantity : 35,
		  unitPrice : 1090 },
		{ line : 2,
		  product : {_id: 3, prodName : 'Produs 3', mu : 'kg', prodCateg : 'Bere',  percVAT  : 0.24 },
		  quantity : 40,
		  unitPrice : 700 },
		{ line : 3,
		  product : {_id: 4, prodName : 'Produs 4', mu : 'l', prodCateg : 'Dulciuri',  percVAT  : .12 },
		  quantity : 55,
		  unitPrice : 1410 },
		{ line : 4,
		  product : {_id: 5, prodName : 'Produs 5', mu : 'buc', prodCateg : 'Tigari',  percVAT  : .24 },
		  quantity : 755,
		  unitPrice : 6300 }  ] }) ;

db.invoices.createIndex({_id : 1}) ;
db.invoices.createIndex({invDate: 1}, {unique: false}) ;
db.invoices.createIndex({codcl: 1}, {unique: false}) ;

db.invoices.find().pretty() ;


//==================================================================================
//
//		                the sixth collection: receipts
//
//==================================================================================

db.receipts.drop()

// receipt 1234
db.receipts.insertOne ( {
		_id : 1234,
		recDate : new ISODate("2019-08-12T11:00:00Z"),
		docCode: 'OP',
		docNo : '121',
		docDate : new ISODate("2019-08-10T11:00:00Z"),
		invoicesCollected : [
			{ invNo : 1111, amount : 4527400 }
			]
		} ) ;


// receipt 1235
db.receipts.insertOne ( { _id : 1235, recDate : new ISODate("2019-08-18T11:00:00Z"),
	docCode: 'CEC', docNo : '212', docDate : new ISODate("2019-08-17T11:00:00Z"),
	invoicesCollected : [
		{ invNo : 1112, amount : 129488 },
		{ invNo : 2112, amount : 158506 }	] } ) ;


// receipt 1236
db.receipts.insertOne ( { _id : 1236, recDate : new ISODate("2019-09-07T11:00:00Z"),
	docCode: 'OP', docNo : '321', docDate : new ISODate("2019-09-05T11:00:00Z"),
	invoicesCollected : [
		{ invNo : 1113, amount : 50000 },
		{ invNo : 2113, amount : 50000 },
		{ invNo : 3113, amount : 50000 }	] } ) ;


// receipt 1237
db.receipts.insertOne ( { _id : 1237, recDate : new ISODate("2019-09-08T11:00:00Z"),
	docCode: 'CEC', docNo : '445', docDate : new ISODate("2019-09-06T11:00:00Z"),
	invoicesCollected : [
		{ invNo : 1114, amount : 6272728 } ] } ) ;


// receipt 1238
db.receipts.insertOne ( { _id : 1238, recDate : new ISODate("2019-09-08T11:00:00Z"),
	docCode: 'OP', docNo : '532', docDate : new ISODate("2019-09-06T11:00:00Z"),
	invoicesCollected : [
		{ invNo : 1115, amount : 70000 },
		{ invNo : 1117, amount : 100000 },
		{ invNo : 1118, amount : 100000 },
		{ invNo : 1120, amount : 70000 }	] } ) ;


// receipt 1239
db.receipts.insertOne ( { _id : 1239, recDate : new ISODate("2019-09-09T11:00:00Z"),
	docCode: 'OP', docNo : '622', docDate : new ISODate("2019-09-07T11:00:00Z"),
	invoicesCollected : [
		{ invNo : 1116, amount : 130200 }	] } ) ;


// receipt 1240
db.receipts.insertOne ( { _id : 1240, recDate : new ISODate("2019-09-09T11:00:00Z"),
	docCode: 'OP', docNo : '432', docDate : new ISODate("2019-09-05T11:00:00Z"),
	invoicesCollected : [
		{ invNo : 1119, amount : 6015408 }	] } ) ;


// receipt 1241
db.receipts.insertOne ( { _id : 1241, recDate : new ISODate("2019-09-18T11:00:00Z"),
	docCode: 'OP', docNo : '213', docDate : new ISODate("2019-09-11T11:00:00Z"),
	invoicesCollected : [
		{ invNo : 1121, amount : 4935248 },
		{ invNo : 2121, amount : 4938776 }	] } ) ;


db.receipts.find().pretty() ;


/*  add a set on invoices, based on existing invoices:
    * changind invoice number (increment by 1000)
    * changing invoice dates (increment by 365)
    * removing a line from two or three invoices
*/

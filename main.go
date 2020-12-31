package main

import (
	"fmt"
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

type Transaction struct {
	ID		  primitive.ObjectID 	`json:"_id,omitempty" bson:"_id,omitempty"`
	Phone     string				`json:"phone,omitempty" bson:"phone,omitempty"`
	Name      string				`json:"name,omitempty" bson:"name,omitempty"`
	Operator  string				`json:"operator,omitempty" bson:"operator,omitempty"`
	Nominal   int					`json:"nominal,omitempty" bson:"nominal,omitempty"`
}

type ErrorResponse struct {
	StatusCode 	 int		`json:"status"`
	ErrorMessage string		`json:"message"`
}

type Operator struct {
	ID			string		`json:"_id,omitempty" bson:"_id,omitempty"`
	Sum			int			`json:"sum,omitempty" bson:"sum,omitempty"`
}

var collection = connectDB()

func main()  {
	router := mux.NewRouter()

	router.HandleFunc("/api/pulsa", readAllTransaction).Methods("GET")
	router.HandleFunc("/api/pulsa/{id}", readTransactionById).Methods("GET")
	router.HandleFunc("/api/pulsa", createTransaction).Methods("POST")
	router.HandleFunc("/api/pulsa/{id}", updateTransaction).Methods("PUT")
	router.HandleFunc("/api/pulsa/{id}", deleteTransaction).Methods("DELETE")
	router.HandleFunc("/api/leaderboard", readLeaderboard).Methods("GET")

	log.Fatal(http.ListenAndServe(":8000", router))
}

func connectDB() *mongo.Collection {
	clientOptions := options.Client().ApplyURI("mongodb://127.0.0.1:27017")

	client, err := mongo.Connect(context.TODO(), clientOptions)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Connected to MongoDB!")

	collection := client.Database("gocrud").Collection("pulsa")

	return collection
}

func getError(err error, w http.ResponseWriter)  {
	var response = ErrorResponse{
		StatusCode: http.StatusInternalServerError,
		ErrorMessage: err.Error(),
	}

	message, _ := json.Marshal(response)

	w.WriteHeader(response.StatusCode)
	w.Write(message)
}

func readAllTransaction(w http.ResponseWriter, r *http.Request)  {
	w.Header().Set("Content-Type", "application/json")

	var transactions [] Transaction
	cur, err := collection.Find(context.TODO(), bson.M{})

	if err != nil {
		log.Fatal(err)
		return
	}

	defer cur.Close(context.TODO())

	for cur.Next(context.TODO()) {
		var transaction Transaction
		err := cur.Decode(&transaction)
		fmt.Println(cur)
		if err != nil {
			log.Fatal(err)
		}

		transactions = append(transactions, transaction)
	}

	if err := cur.Err(); err != nil {
		log.Fatal(err)
	}

	json.NewEncoder(w).Encode(transactions)
}

func readTransactionById(w http.ResponseWriter, r *http.Request)  {
	w.Header().Set("Content-Type", "appplication/json")

	var transaction Transaction
	var params = mux.Vars(r)

	id, _ := primitive.ObjectIDFromHex(params["id"])

	filter := bson.M{"_id": id}
	err := collection.FindOne(context.TODO(), filter).Decode(&transaction)

	if err != nil {
		getError(err, w)
		return
	}

	json.NewEncoder(w).Encode(transaction)
}

func createTransaction(w http.ResponseWriter, r *http.Request)  {
	w.Header().Set("Content-Type", "appplication/json")

	var transaction Transaction
	_ = json.NewDecoder(r.Body).Decode(&transaction)

	result, err := collection.InsertOne(context.TODO(), transaction)

	if err != nil {
		getError(err, w)
		return
	}

	json.NewEncoder(w).Encode(result)
}

func updateTransaction(w http.ResponseWriter, r *http.Request)  {
	w.Header().Set("Content-Type", "appplication/json")

	var params = mux.Vars(r)

	id, _ := primitive.ObjectIDFromHex(params["id"])

	var transaction Transaction

	filter := bson.M{"_id": id}

	_ = json.NewDecoder(r.Body).Decode(&transaction)

	update := bson.D{
		{
			"$set", bson.D{
				{"phone", transaction.Phone},
				{"name", transaction.Name},
				{"operator", transaction.Operator}},
		}}

	err := collection.FindOneAndUpdate(context.TODO(), filter, update).Decode(&transaction)

	if err != nil {
		getError(err, w)
		return
	}

	json.NewEncoder(w).Encode(transaction)
}

func deleteTransaction(w http.ResponseWriter, r *http.Request)  {
	w.Header().Set("Content-Type", "appplication/json")

	var params = mux.Vars(r)

	id, err := primitive.ObjectIDFromHex(params["id"])

	filter := bson.M{"_id": id}

	deleteResult, err := collection.DeleteOne(context.TODO(), filter)

	if err != nil {
		log.Fatal(err, w)
	}

	json.NewEncoder(w).Encode(deleteResult)
}

func readLeaderboard(w http.ResponseWriter, r *http.Request)  {
	w.Header().Set("Content-Type", "application/json")
	
	var operators [] Operator
	groupCommand := bson.D{
		{"$group", bson.D {
			{"_id", "$operator"},
			{"sum", bson.D{
				{"$sum", "$nominal"},
			}},
		}},
	}
	sortCommand := bson.D{
		{"$sort", bson.D{
			{"sum", -1},
		}},
	}
	opts := options.Aggregate().SetMaxTime(2 * 5)
	cur, err := collection.Aggregate(context.TODO(), mongo.Pipeline{groupCommand, sortCommand}, opts)

	if err != nil {
		log.Fatal(err)
		return
	}

	defer cur.Close(context.TODO())

	for cur.Next(context.TODO()) {
		var operator Operator
		err := cur.Decode(&operator)
		if err != nil {
			log.Fatal(err)
		}

		operators = append(operators, operator)
	}

	if err := cur.Err(); err != nil {
		getError(err, w)
		return
	}

	json.NewEncoder(w).Encode(operators)
}

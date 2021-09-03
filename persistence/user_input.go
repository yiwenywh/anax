package persistence

import (
	"fmt"
	"github.com/open-horizon/anax/policy"
)

const NODE_USERINPUT = "nodeuserinput"                              // The bucket name in the bolt DB.
const EXCHANGE_NODE_USERINPUT_HASH = "exchange_node_userinput_hash" // The buucket for the exchange node userinput hash

// Retrieve the node user input object from the database. The bolt APIs assume there is more than 1 object in a bucket,
// so this function has to be prepared for that case, even though there should only ever be 1.
func FindNodeUserInput(db AgentDatabase) ([]policy.UserInput, error) {
	return db.FindNodeUserInput()
}

// There is only 1 object in the bucket so we can use the bucket name as the object key.
func SaveNodeUserInput(db AgentDatabase, userInput []policy.UserInput) error {
	return db.SaveNodeUserInput(userInput)
}

// Remove the node user input object from the local database.
func DeleteNodeUserInput(db AgentDatabase) error {
	return db.DeleteNodeUserInput()
}

// Delete all UserInputAttributes from the local db.
func DeleteAllUserInputAttributes(db AgentDatabase) error {
	if attributes, err := FindApplicableAttributes(db, "", ""); err != nil {
		return fmt.Errorf("Failed to get all the UserInputAttributes. %v", err)
	} else {
		for _, attr := range attributes {
			switch attr.(type) {
			case UserInputAttributes:
				if _, err := DeleteAttribute(db, attr.GetMeta().Id); err != nil {
					return fmt.Errorf("Failed to delete UserInputAttributes %v. %v", attr, err)
				}
			}
		}
	}

	return nil
}

// Get all UerInputAttributes from db
func GetAllUserInputAttributes(db AgentDatabase) ([]UserInputAttributes, error) {
	allUI := []UserInputAttributes{}
	if attributes, err := FindApplicableAttributes(db, "", ""); err != nil {
		return nil, fmt.Errorf("Failed to get all the UserInputAttributes. %v", err)
	} else {
		for _, attr := range attributes {
			switch attr.(type) {
			case UserInputAttributes:
				allUI = append(allUI, attr.(UserInputAttributes))
			}
		}
	}

	return allUI, nil
}

// Retrieve the exchange node user input hash from the database.
func GetNodeUserInputHash_Exch(db AgentDatabase) ([]byte, error) {
	return db.GetNodeUserInputHash_Exch()
}

// save the exchange node user input hash.
func SaveNodeUserInputHash_Exch(db AgentDatabase, userInputHash []byte) error {
	return db.SaveNodeUserInputHash_Exch(userInputHash)
}

// Remove the exchange node user input hash from the local database.
func DeleteNodeUserInputHash_Exch(db AgentDatabase) error {
	return db.DeleteNodeUserInputHash_Exch()
}

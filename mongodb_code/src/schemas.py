# schemas.py

# User schema
user_schema = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["_id", "has_labels"],
            "properties": {
                "_id": {
                    "bsonType": "string",
                    "description": "must be a string and is required",
                },
                "has_labels": {
                    "bsonType": "bool",
                    "description": "must be a boolean and is required",
                },
            },
        }
    },
    "validationLevel": "strict",
}

# Activity schema
activity_schema = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["user_id", "start_date_time"],
            "properties": {
                "user_id": {
                    "bsonType": "string",
                    "description": "must be a string and is required",
                },
                "start_date_time": {
                    "bsonType": "date",
                    "description": "must be a date and is required",
                },
                "transportation_mode": {
                    "bsonType": "string",
                    "description": "transportation mode; optional",
                },
            },
        }
    },
    "validationLevel": "strict",
}

# TrackPoint schema
trackpoint_schema = {
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["activity_id", "date_time", "location", "altitude"],
            "properties": {
                "activity_id": {
                    "bsonType": "string",
                    "description": "must be a string and is required",
                },
                "user_id": {
                    "bsonType": "string",
                    "description": "must be a string and is required",
                },
                "date_time": {
                    "bsonType": "date",
                    "description": "must be a date and is required",
                },
                "location": {
                    "bsonType": "object",
                    "required": ["type", "coordinates"],
                    "properties": {
                        "type": {
                            "enum": ["Point"],
                            "description": "must be 'Point' as a GeoJSON object type",
                        },
                        "coordinates": {
                            "bsonType": "array",
                            "minItems": 2,
                            "maxItems": 2,
                            "items": [
                                {"bsonType": "double", "description": "longitude"},
                                {"bsonType": "double", "description": "latitude"},
                            ],
                            "description": "must be an array of [longitude, latitude]",
                        },
                    },
                    "description": "GeoJSON Point with longitude and latitude",
                },
                "altitude": {
                    "bsonType": "int",
                    "description": "altitude in (Feet) integer format",
                },
            },
        }
    },
    "validationLevel": "strict",
}

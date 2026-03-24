package adapty

type Event struct {
	ProfileID                  string                 `json:"profile_id"`
	CustomerUserID             *string                `json:"customer_user_id"`
	IDFV                       *string                `json:"idfv"`
	IDFA                       *string                `json:"idfa"`
	AdvertisingID              *string                `json:"advertising_id"`
	ProfileInstallDatetime     *string                `json:"profile_install_datetime"`
	UserAgent                  *string                `json:"user_agent"`
	Email                      *string                `json:"email"`
	EventType                  string                 `json:"event_type"`
	EventDatetime              string                 `json:"event_datetime"`
	EventProperties            map[string]interface{} `json:"event_properties"`
	EventAPIVersion            *int                   `json:"event_api_version"`
	ProfilesSharingAccessLevel []ProfileShare         `json:"profiles_sharing_access_level"`
	Attributions               map[string]Attribution `json:"attributions"`
	UserAttributes             map[string]interface{} `json:"user_attributes"`
	IntegrationIDs             map[string]string      `json:"integration_ids"`
	PlayStorePurchaseToken     *PlayStorePurchaseToken `json:"play_store_purchase_token"`
}

type ProfileShare struct {
	ProfileID      string  `json:"profile_id"`
	CustomerUserID *string `json:"customer_user_id"`
}

type Attribution struct {
	AdSet         *string `json:"ad_set"`
	Status        *string `json:"status"`
	Channel       *string `json:"channel"`
	AdGroup       *string `json:"ad_group"`
	Campaign      *string `json:"campaign"`
	Creative      *string `json:"creative"`
	CreatedAt     *string `json:"created_at"`
	NetworkUserID *string `json:"network_user_id"`
}

type PlayStorePurchaseToken struct {
	ProductID      string `json:"product_id"`
	PurchaseToken  string `json:"purchase_token"`
	IsSubscription bool   `json:"is_subscription"`
}

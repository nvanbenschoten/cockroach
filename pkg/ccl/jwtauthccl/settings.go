// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package jwtauthccl

import (
	"bytes"
	"crypto/x509"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/jwk"
)

// All cluster settings necessary for the JWT authentication feature.
const (
	baseJWTAuthSettingName           = "server.jwt_authentication."
	JWTAuthAudienceSettingName       = baseJWTAuthSettingName + "audience"
	JWTAuthEnabledSettingName        = baseJWTAuthSettingName + "enabled"
	JWTAuthIssuersSettingName        = baseJWTAuthSettingName + "issuers"
	JWTAuthJWKSSettingName           = baseJWTAuthSettingName + "jwks"
	JWTAuthClaimSettingName          = baseJWTAuthSettingName + "claim"
	JWKSAutoFetchEnabledSettingName  = baseJWTAuthSettingName + "jwks_auto_fetch.enabled"
	jwtAuthIssuerCustomCASettingName = baseJWTAuthSettingName + "issuers.custom_ca"
)

// JWTAuthClaim sets the JWT claim that is parsed to get the username.
var JWTAuthClaim = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	JWTAuthClaimSettingName,
	"sets the JWT claim that is parsed to get the username",
	"",
	settings.WithReportable(true),
)

// JWTAuthAudience sets accepted audience values for JWT logins over the SQL interface.
var JWTAuthAudience = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	JWTAuthAudienceSettingName,
	"sets accepted audience values for JWT logins over the SQL interface",
	"",
	settings.WithValidateString(validateJWTAuthAudiences),
)

// JWTAuthEnabled enables or disabled JWT login over the SQL interface.
var JWTAuthEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	JWTAuthEnabledSettingName,
	"enables or disables JWT login for the SQL interface",
	false,
	settings.WithReportable(true),
)

// JWTAuthJWKS is the public key set for JWT logins over the SQL interface.
var JWTAuthJWKS = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	JWTAuthJWKSSettingName,
	"sets the public key set for JWT logins over the SQL interface (JWKS format)",
	"{\"keys\":[]}",
	settings.WithValidateString(validateJWTAuthJWKS),
)

// JWTAuthIssuers is the list of "issuer" values that are accepted for JWT logins over the SQL interface.
var JWTAuthIssuers = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	JWTAuthIssuersSettingName,
	"sets accepted issuer values for JWT logins over the SQL interface either as a string or as a JSON "+
		"string with an array of issuer strings in it",
	"",
	settings.WithValidateString(validateJWTAuthIssuers),
)

// JWTAuthIssuerCustomCA is the custom root CA for verifying certificates while
// fetching JWKS from the JWT issuers.
var JWTAuthIssuerCustomCA = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	jwtAuthIssuerCustomCASettingName,
	"sets the PEM encoded custom root CA for verifying certificates while fetching JWKS",
	"",
	settings.WithReportable(false),
	settings.Sensitive,
	settings.WithValidateString(validateJWTAuthIssuerCACert),
)

// JWKSAutoFetchEnabled enables or disables automatic fetching of JWKs from the issuer's well-known endpoint.
var JWKSAutoFetchEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	JWKSAutoFetchEnabledSettingName,
	"enables or disables automatic fetching of JWKs from the issuer's well-known endpoint. "+
		"If this is enabled, the server.jwt_authentication.jwks will be ignored.",
	false,
	settings.WithReportable(true),
)

func validateJWTAuthIssuers(values *settings.Values, s string) error {
	var issuers []string

	var jsonCheck json.RawMessage
	if json.Unmarshal([]byte(s), &jsonCheck) != nil {
		// If we know the string is *not* valid JSON, fall back to assuming basic
		// string to use a single valid issuer
		return nil
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(s)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&issuers); err != nil {
		return errors.Wrap(err, "JWT authentication issuers JSON not valid")
	}
	return nil
}

func validateJWTAuthAudiences(values *settings.Values, s string) error {
	var audiences []string

	var jsonCheck json.RawMessage
	if json.Unmarshal([]byte(s), &jsonCheck) != nil {
		// If we know the string is *not* valid JSON, fall back to assuming basic
		// string to use a single valid issuer
		return nil
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(s)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&audiences); err != nil {
		return errors.Wrap(err, "JWT authentication audiences JSON not valid")
	}
	return nil
}

func validateJWTAuthJWKS(values *settings.Values, s string) error {
	if _, err := jwk.Parse([]byte(s)); err != nil {
		return errors.Wrap(err, "JWT authentication JWKS not a valid JWKS")
	}
	return nil
}

func mustParseValueOrArray(rawString string) []string {
	var array []string

	var jsonCheck json.RawMessage
	if json.Unmarshal([]byte(rawString), &jsonCheck) != nil {
		// If we know the string is *not* valid JSON, fall back to assuming basic
		// string to use a single valid.
		return []string{rawString}
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(rawString)))
	if err := decoder.Decode(&array); err != nil {
		return []string{rawString}
	}
	return array
}

func mustParseJWKS(jwks string) jwk.Set {
	keySet, err := jwk.Parse([]byte(jwks))
	if err != nil {
		return jwk.NewSet()
	}
	return keySet
}

func validateJWTAuthIssuerCACert(values *settings.Values, s string) error {
	if len(s) != 0 {
		if ok := x509.NewCertPool().AppendCertsFromPEM([]byte(s)); !ok {
			return errors.Newf("JWT authentication issuer custom CA certificate not valid")
		}
	}
	return nil
}

// Copyright (c) 2019-2023, Sylabs Inc. All rights reserved.
// This software is licensed under a 3-clause BSD license. Please consult the
// LICENSE.md file distributed with the sources of this project regarding your
// rights to use or distribute this software.

package singularity

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/sylabs/scs-library-client/client"
	"github.com/sylabs/singularity/internal/pkg/remote"
	"github.com/sylabs/singularity/internal/pkg/remote/credential"
	"github.com/sylabs/singularity/internal/pkg/remote/endpoint"
	useragent "github.com/sylabs/singularity/pkg/util/user-agent"
)

// RemoteGetPassword gets an OCI password
func RemoteGetPassword(usrConfigFile string, libraryConfig *client.Config) (err error) {
	c := &remote.Config{}

	// opening config file
	file, err := os.OpenFile(usrConfigFile, os.O_RDONLY|os.O_CREATE, 0o600)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no remote configurations")
		}
		return fmt.Errorf("while opening remote config file: %s", err)
	}
	defer file.Close()

	// read file contents to config struct
	c, err = remote.ReadFrom(file)
	if err != nil {
		return fmt.Errorf("while parsing remote config data: %s", err)
	}

	if err := syncSysConfig(c); err != nil {
		return err
	}

	var r *endpoint.Config
	r, err = c.GetDefault()

	cs, err := r.GetServiceURI(endpoint.Consent)
	if err != nil {
		return fmt.Errorf("error getting consent uri: %v", err)
	}

	httpclient := &http.Client{
		Timeout: 10 * time.Second,
	}

	// This is static user ID to POC the approach
	req, err := http.NewRequest(http.MethodGet, cs+"/v1/admin/users/63d2f69412013461b733aade", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", credential.TokenPrefix+r.Token)
	req.Header.Set("User-Agent", useragent.Value())

	res, err := httpclient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request to server: %v", err)
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("error making request to server: %v", err)
	}

	type userInfo struct {
		Data struct {
			Username string `json:"username"`
		} `json:"data"`
	}
	var userinfo userInfo
	err = json.Unmarshal(body, &userinfo)
	if err != nil {
	}

	libraryClient, err := client.NewClient(libraryConfig)
	if err != nil {
		return fmt.Errorf("error initializing library client: %v", err)
	}

	host, token, err := libraryClient.OciRegistryToken(context.Background(), userinfo.Data.Username)
	if err != nil {
		return fmt.Errorf("error getting registry token: %v", err)
	}

	req, err = http.NewRequest(http.MethodGet, host.String()+"/api/v2.0/users/current", nil)
	if err != nil {
		return err
	}

	err = token.ModifyRequest(req)
	if err != nil {
		return err
	}

	res, err = httpclient.Do(req)
	if err != nil {
		return fmt.Errorf("error making request to server: %v", err)
	}
	body, err = ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("error making request to server: %v", err)
	}

	type harborInfo struct {
		Oidc struct {
			Secret string `json:"secret"`
		} `json:"oidc_user_meta"`
	}
	var oci harborInfo
	err = json.Unmarshal(body, &oci)
	if err != nil {
	}

	fmt.Printf("%v\n", oci.Oidc.Secret)

	return nil
}

package utils

import (
	"NewStatis6.0/config"
	"NewStatis6.0/logger"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// ReqData2 对应采集云登录接口的响应结构。
type ReqData2 struct {
	Code   int       `json:"code"`
	Msg    string    `json:"msg"`
	Return tokenData `json:"return"`
}

// tokenData 保存登录成功后返回的访问令牌。
type tokenData struct {
	Token string `json:"token"`
}

// GetResData 对应采集云网关列表接口的响应结构。
type GetResData struct {
	Code   int       `json:"code"`
	Msg    string    `json:"msg"`
	Return GetReturn `json:"return"`
}

// GetReturn 保存采集云返回的网关总数和网关列表。
type GetReturn struct {
	Acount  int          `json:"acount"`
	Gateway []GetGateway `json:"gateway"`
}

// GetGateway 描述一个可用于创建统计器的采集网关。
type GetGateway struct {
	Admin     string `json:"admin"`
	Name      string `json:"name"`
	GatewayID string `json:"gatewayID"`
	Model     string `json:"model"`
	Card      string `json:"card"`
}

// resPoint 对应采集云测点列表接口的响应结构。
type resPoint struct {
	Code   int          `json:"code"`
	Msg    string       `json:"msg"`
	Return resPointData `json:"return"`
}

// resPointData 包装采集云返回的测点数量和列表。
type resPointData struct {
	Account int      `json:"account"`
	Data    DataList `json:"data"`
}

// DataList 是采集云测点列表的数据容器。
type DataList struct {
	List []Point `json:"list"`
}

// Point 描述采集云中的测点标识和名称。
type Point struct {
	DataID string `json:"dataID"`
	Name   string `json:"name"`
}

// LoginAndGetToken 使用配置账号登录采集云并返回 Bearer Token。
func LoginAndGetToken() (string, error) {
	url := strings.TrimRight(config.Conf.CJY.BaseURL, "/") + "/go/auth/login"
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		url = "http://" + url
	}

	body, err := json.Marshal(map[string]string{
		"name":     config.Conf.CJY.Username,
		"password": config.Conf.CJY.Password,
	})
	if err != nil {
		return "", err
	}
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("login failed: %s", string(respBody))
	}
	var result ReqData2
	if err = json.Unmarshal(respBody, &result); err != nil {
		return "", err
	}
	if result.Return.Token == "" {
		return "", fmt.Errorf("login token is empty")
	}
	return result.Return.Token, nil
}

// GetGatewayData 使用登录令牌读取全部可用网关。
func GetGatewayData(token string) ([]GetGateway, error) {
	url := cjyURL("/go/gateway/findpage?Pagenum=1&Pagesize=100000&Orderobject=_id%20%20%20%20&Ordervalue=-1&Function=intact&STR=STR")
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+token)

	var data GetResData
	if err = doCJYRequest(req, &data); err != nil {
		return nil, err
	}
	return data.Return.Gateway, nil
}

// GetPointData 读取指定网关下可用于统计的测点列表。
func GetPointData(token string, gatewayID string) ([]Point, error) {
	url := cjyURL("/go/gateway/getdata?GatewayID=" + gatewayID + "&Function=list")
	req, err := http.NewRequest(http.MethodPost, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", "Bearer "+token)

	var data resPoint
	if err = doCJYRequest(req, &data); err != nil {
		return nil, err
	}
	return data.Return.Data.List, nil
}

// cjyURL 规范化采集云基础地址，并拼接接口路径。
func cjyURL(path string) string {
	base := strings.TrimRight(config.Conf.CJY.BaseURL, "/")
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "http://" + base
	}
	return base + path
}

// doCJYRequest 以固定超时执行采集云请求，校验状态码并解析 JSON 响应。
func doCJYRequest(req *http.Request, dst interface{}) error {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("cjy request failed: %s", string(body))
	}
	if err = json.Unmarshal(body, dst); err != nil {
		logger.Error("cjy json unmarshal failed", err)
		return err
	}
	return nil
}

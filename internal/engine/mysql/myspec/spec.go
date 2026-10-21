// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package myspec defines the dependency-free, secret-bearing MySQL member and action protocol.
package myspec

import (
	"bytes"
	"encoding/pem"
	"fmt"
	"net"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	Engine                             = "mysql"
	RoleData                           = "data"
	RoleProxy                          = "proxy"
	TopologySingle                     = "single"
	TopologyGroup                      = "group"
	PortMySQL                          = 3306
	PortProxy                          = 3306
	PortProxyReader                    = 3307
	PortProxyAdmin                     = 6032
	PortAgent                          = 9443
	ProxyWriterGroup                   = 10
	ProxyBackupWriterGroup             = 11
	ProxyReaderGroup                   = 20
	ProxyOfflineGroup                  = 30
	UserRoot                           = "root"
	UserAgent                          = "kk_agent"
	UserReplication                    = "kk_repl"
	UserMonitor                        = "kk_monitor"
	UserBackup                         = "kk_backup"
	ConsistencyBeforeOnPrimaryFailover = "BEFORE_ON_PRIMARY_FAILOVER"
	ActionStatus                       = "status"
	ActionOfflineGTID                  = "offline-gtid"
	ActionSwitchover                   = "switchover"
	ActionBootstrap                    = "bootstrap"
	ActionStartGroup                   = "start-group"
	ActionStopGroup                    = "stop-group"
	ActionClone                        = "clone"
	ActionBackup                       = "backup"
	ActionBackupInfo                   = "backup-info"
	ActionArchiveStatus                = "archive-status"
	ActionPITRCoverage                 = "pitr-coverage"
	ActionRestore                      = "restore"
	ActionRestorePreflight             = "restore-preflight"
	ActionRestoreStatus                = "restore-status"
	ActionProxyStatus                  = "proxy-status"
)

type Certificate struct {
	CertPEM string `json:"certPEM"`
	KeyPEM  string `json:"keyPEM"`
}
type Credentials struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type Peer struct {
	Name string `json:"name"`
	Host string `json:"host"`
	Port int    `json:"port"`
}
type GroupReplication struct {
	GroupName      string `json:"groupName"`
	FormationEpoch uint64 `json:"formationEpoch"`
	// Bootstrap permits this start only; it is never a persistent server setting.
	Bootstrap                         bool   `json:"bootstrap"`
	Seeds                             []Peer `json:"seeds"`
	Consistency                       string `json:"consistency"`
	UnreachableMajorityTimeoutSeconds int    `json:"unreachableMajorityTimeoutSeconds"`
	AutorejoinTries                   int    `json:"autorejoinTries"`
	MemberWeight                      int    `json:"memberWeight"`
	CloneDonor                        *Peer  `json:"cloneDonor,omitempty"`
	// Reclone explicitly authorizes replacing an existing member during recovery.
	Reclone        bool   `json:"reclone,omitempty"`
	RecloneReceipt string `json:"recloneReceipt,omitempty"`
}
type S3 struct {
	Endpoint  string `json:"endpoint"`
	Port      int    `json:"port,omitempty"`
	Bucket    string `json:"bucket"`
	Region    string `json:"region"`
	PathStyle bool   `json:"pathStyle"`
	VerifyTLS bool   `json:"verifyTls"`
	CAPEM     string `json:"caPEM,omitempty"`
	AccessKey string `json:"accessKey"`
	SecretKey string `json:"secretKey"`
}
type Repository struct {
	S3   S3     `json:"s3"`
	Path string `json:"path"`
}
type Binlog struct {
	Enabled               bool `json:"enabled"`
	RotateIntervalSeconds int  `json:"rotateIntervalSeconds"`
	ExpireSeconds         int  `json:"expireSeconds"`
}
type Backup struct {
	Repository Repository `json:"repository"`
	Binlog     Binlog     `json:"binlog"`
}
type Restore struct {
	Source          Repository          `json:"source"`
	BackupID        string              `json:"backupId"`
	Label           string              `json:"label"`
	ObjectPaths     []string            `json:"objectPaths"`
	ObjectManifests []BackupManifestRef `json:"objectManifests"`
	TargetGTID      string              `json:"targetGTID,omitempty"`
	TargetTime      string              `json:"targetTime,omitempty"`
	NewGroupName    string              `json:"newGroupName,omitempty"`
	// RecoveryUsers is ephemeral secret material; never persist it in CP checkpoints.
	RecoveryUsers []RecoveryUser `json:"recoveryUsers,omitempty"`
}
type RecoveryUser struct {
	Name      string `json:"name"`
	Current   string `json:"current"`
	Secondary string `json:"secondary,omitempty"`
}
type ProxyUser struct {
	Username   string `json:"username"`
	Password   string `json:"password"`
	RequireTLS bool   `json:"requireTls"`
}
type Proxy struct {
	Mode           string      `json:"mode,omitempty"`
	Backends       []Peer      `json:"backends"`
	Users          []ProxyUser `json:"users"`
	Monitor        Credentials `json:"monitor"`
	Admin          Credentials `json:"admin"`
	ReadWriteSplit bool        `json:"readWriteSplit"`
	ReaderPort     bool        `json:"readerPort"`
	MaxConnections int         `json:"maxConnections"`
}
type Spec struct {
	Member           string            `json:"member"`
	Role             string            `json:"role"`
	Topology         string            `json:"topology"`
	Ordinal          int               `json:"ordinal"`
	Version          string            `json:"version"`
	ServerDir        string            `json:"serverDir"`
	XtraBackupDir    string            `json:"xtraBackupDir"`
	BufferPoolBytes  int64             `json:"bufferPoolBytes"`
	MaxConnections   int               `json:"maxConnections"`
	Parameters       map[string]string `json:"parameters,omitempty"`
	Peers            []Peer            `json:"peers,omitempty"`
	GroupReplication *GroupReplication `json:"groupReplication,omitempty"`
	Galera           *Galera           `json:"galera,omitempty"`
	// AllowInitialize authorizes creation of a new data identity, never recovery of a formed member.
	AllowInitialize bool `json:"allowInitialize,omitempty"`
	// SlowShutdown prepares a clean major-version datadir upgrade before runtime replacement.
	SlowShutdown bool         `json:"slowShutdown,omitempty"`
	Superuser    Credentials  `json:"superuser"`
	Admin        Credentials  `json:"admin"`
	Agent        Credentials  `json:"agent"`
	Replication  Credentials  `json:"replication"`
	Monitor      Credentials  `json:"monitor"`
	BackupUser   Credentials  `json:"backupUser"`
	CAPEM        string       `json:"caPEM"`
	ServerCert   *Certificate `json:"serverCert"`
	Proxy        *Proxy       `json:"proxy,omitempty"`
	Backup       *Backup      `json:"backup,omitempty"`
	Restore      *Restore     `json:"restore,omitempty"`
}
type DataIdentity struct {
	Initialized    bool   `json:"initialized"`
	ServerUUID     string `json:"serverUUID"`
	FormationEpoch uint64 `json:"formationEpoch"`
}
type StatusResponse struct {
	Role             string       `json:"role"`
	MemberState      string       `json:"memberState"`
	MemberRole       string       `json:"memberRole"`
	GroupMembers     int          `json:"groupMembers,omitempty"`
	ServerUUID       string       `json:"serverUUID"`
	GTIDExecuted     string       `json:"gtidExecuted"`
	ReadOnly         bool         `json:"readOnly"`
	SuperReadOnly    bool         `json:"superReadOnly"`
	Version          string       `json:"version"`
	DataIdentity     DataIdentity `json:"dataIdentity"`
	PhysicalIdentity string       `json:"physicalIdentity,omitempty"`
}
type OfflineGTIDResponse struct {
	// Stopped confirms this observation was obtained without a running server.
	Stopped      bool         `json:"stopped"`
	ServerUUID   string       `json:"serverUUID"`
	GTIDExecuted string       `json:"gtidExecuted"`
	DataIdentity DataIdentity `json:"dataIdentity"`
}
type SwitchoverRequest struct {
	TargetUUID string `json:"targetUUID"`
}
type GroupRequest struct {
	GroupName      string `json:"groupName"`
	FormationEpoch uint64 `json:"formationEpoch"`
	Bootstrap      bool   `json:"bootstrap"`
}
type CloneRequest struct {
	Receipt string `json:"receipt"`
	Donor   Peer   `json:"donor"`
	Version string `json:"version"`
}
type BackupRequest struct {
	Receipt          string     `json:"receipt"`
	Type             string     `json:"type"`
	BaseLSN          uint64     `json:"baseLSN,omitempty"`
	Repository       Repository `json:"repository"`
	ObjectPrefix     string     `json:"objectPrefix"`
	PhysicalIdentity string     `json:"physicalIdentity,omitempty"`
}
type BackupResult struct {
	GTIDSet          string    `json:"gtidSet"`
	SourceUUID       string    `json:"sourceUUID,omitempty"`
	BinlogFile       string    `json:"binlogFile,omitempty"`
	BinlogPosition   uint64    `json:"binlogPosition,omitempty"`
	FromLSN          uint64    `json:"fromLSN"`
	ToLSN            uint64    `json:"toLSN"`
	EndTime          time.Time `json:"endTime"`
	ServerVersion    string    `json:"serverVersion"`
	ObjectPrefix     string    `json:"objectPrefix"`
	Size             int64     `json:"size"`
	BackupType       string    `json:"backupType"`
	ToolVersion      string    `json:"toolVersion"`
	PhysicalIdentity string    `json:"physicalIdentity,omitempty"`
	ManifestDigest   string    `json:"manifestDigest"`
	ManifestSize     int64     `json:"manifestSize"`
}
type BackupResponse struct {
	Receipt string        `json:"receipt"`
	State   string        `json:"state"`
	Result  *BackupResult `json:"result,omitempty"`
	Error   string        `json:"error,omitempty"`
}
type BinlogArchiveStatus struct {
	LastArchivedFile  string    `json:"lastArchivedFile"`
	GTIDSet           string    `json:"gtidSet"`
	LastEventTime     time.Time `json:"lastEventTime"`
	LagSeconds        float64   `json:"lagSeconds"`
	Gap               bool      `json:"gap"`
	GapReason         string    `json:"gapReason,omitempty"`
	ContiguousEndTime time.Time `json:"contiguousEndTime"`
	ContiguousGTIDSet string    `json:"contiguousGTIDSet"`
}
type RestoreRequest struct {
	Receipt    string  `json:"receipt"`
	Restore    Restore `json:"restore"`
	PlanDigest string  `json:"planDigest,omitempty"`
	RecheckID  string  `json:"recheckID,omitempty"`
}
type RestoreStatus struct {
	Receipt           string   `json:"receipt"`
	State             string   `json:"state"`
	GTIDExecuted      string   `json:"gtidExecuted"`
	Error             string   `json:"error,omitempty"`
	Stage             string   `json:"stage,omitempty"`
	ServerUUID        string   `json:"serverUUID,omitempty"`
	RecoveredUsers    []string `json:"recoveredUsers"`
	PlanDigest        string   `json:"planDigest,omitempty"`
	RecheckID         string   `json:"recheckID,omitempty"`
	VerifiedRecheckID string   `json:"verifiedRecheckID,omitempty"`
}
type ProxyBackend struct {
	Hostgroup       int                 `json:"hostgroup"`
	Host            string              `json:"host"`
	Port            int                 `json:"port"`
	Status          string              `json:"status"`
	ConnectionsUsed int                 `json:"connectionsUsed"`
	ConnectionsFree int                 `json:"connectionsFree"`
	Diagnostics     []ProxyMonitorError `json:"diagnostics,omitempty"`
}

// ProxyMonitorError contains only a closed class, never native text or credentials.
type ProxyMonitorError struct {
	Check         string `json:"check"`
	Class         string `json:"class"`
	TimeUnixMicro int64  `json:"timeUnixMicro"`
}
type ProxyStatusResponse struct {
	Backends []ProxyBackend `json:"backends"`
	// TLSReloadFallback reports that the latest certificate rotation required a process restart.
	TLSReloadFallback bool `json:"tlsReloadFallback,omitempty"`
}

var memberName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
var uuid = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
var version = regexp.MustCompile(`^8\.(0|4)\.[0-9]+-[0-9]+$`)
var hostLabel = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?$`)

func ValidUUID(v string) bool {
	return uuid.MatchString(v) && v != "00000000-0000-0000-0000-000000000000"
}
func validHost(h string) bool {
	if net.ParseIP(h) != nil {
		return true
	}
	if len(h) == 0 || len(h) > 253 {
		return false
	}
	for _, label := range strings.Split(h, ".") {
		if len(label) == 0 || len(label) > 63 || !hostLabel.MatchString(label) {
			return false
		}
	}
	return true
}
func (p Peer) Validate() error {
	if !memberName.MatchString(p.Name) || !validHost(p.Host) || p.Port < 1 || p.Port > 65535 {
		return fmt.Errorf("invalid peer identity, host or port")
	}
	return nil
}
func (c Credentials) Validate() error {
	if c.Username == "" || c.Password == "" || strings.ContainsAny(c.Username+c.Password, "\x00\r\n") {
		return fmt.Errorf("invalid credentials")
	}
	return nil
}
func (c Certificate) Validate() error {
	cert, _ := pem.Decode([]byte(c.CertPEM))
	key, _ := pem.Decode([]byte(c.KeyPEM))
	if cert == nil || cert.Type != "CERTIFICATE" || key == nil || !strings.HasSuffix(key.Type, "PRIVATE KEY") {
		return fmt.Errorf("certificate and private key PEM are required")
	}
	return nil
}
func ValidConsistency(s string) bool {
	switch s {
	case "BEFORE_ON_PRIMARY_FAILOVER", "EVENTUAL", "BEFORE", "AFTER", "BEFORE_AND_AFTER":
		return true
	}
	return false
}
func (g GroupReplication) Validate() error {
	if !ValidUUID(g.GroupName) || g.FormationEpoch == 0 || !ValidConsistency(g.Consistency) || g.UnreachableMajorityTimeoutSeconds < 1 || g.AutorejoinTries < 0 || g.AutorejoinTries > 2016 || g.MemberWeight < 0 || g.MemberWeight > 100 || len(g.Seeds) == 0 {
		return fmt.Errorf("invalid Group Replication settings")
	}
	for _, p := range g.Seeds {
		if err := p.Validate(); err != nil {
			return err
		}
	}
	if g.CloneDonor != nil {
		return g.CloneDonor.Validate()
	}
	return nil
}
func safeObject(s string) bool {
	return s != "" && s != "." && !strings.HasPrefix(s, "/") && path.Clean(s) == s && s != ".." && !strings.HasPrefix(s, "../") && !strings.ContainsAny(s, "\x00\r\n\\")
}
func (r Repository) Validate() error {
	s := r.S3
	u, err := url.Parse(s.Endpoint)
	// Like pgspec, a bare endpoint host is accepted when a separate port is provided.
	endpointOK := err == nil && ((u.Scheme == "https" || u.Scheme == "http") && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == "" || validHost(s.Endpoint))
	if err == nil && u.Port() != "" {
		port, portErr := strconv.Atoi(u.Port())
		endpointOK = endpointOK && portErr == nil && port > 0 && port <= 65535
	}
	if !endpointOK || s.Port < 0 || s.Port > 65535 || s.Bucket == "" || strings.ContainsAny(s.Bucket, "/\x00\r\n") || s.Region == "" || s.AccessKey == "" || s.SecretKey == "" || strings.ContainsAny(s.AccessKey+s.SecretKey, "\x00\r\n") || !safeObject(r.Path) {
		return fmt.Errorf("invalid backup repository")
	}
	return nil
}
func (b Binlog) Validate() error {
	if b.RotateIntervalSeconds < 1 || b.ExpireSeconds <= b.RotateIntervalSeconds {
		return fmt.Errorf("binlog expiry must exceed positive rotation interval")
	}
	return nil
}
func (r Restore) Validate() error {
	if err := r.Source.Validate(); err != nil {
		return err
	}
	if err := ValidateBackupManifestRefs(r.ObjectPaths, r.ObjectManifests); err != nil {
		return err
	}
	if r.BackupID == "" || r.Label == "" || len(r.ObjectPaths) == 0 || r.TargetGTID != "" && r.TargetTime != "" {
		return fmt.Errorf("invalid restore intent")
	}
	for _, p := range r.ObjectPaths {
		if !safeObject(p) {
			return fmt.Errorf("invalid restore object path")
		}
	}
	if r.NewGroupName != "" && !ValidUUID(r.NewGroupName) && !ValidGaleraClusterName(r.NewGroupName) {
		return fmt.Errorf("invalid restored group name")
	}
	if r.TargetTime != "" {
		if _, err := time.Parse(time.RFC3339, r.TargetTime); err != nil {
			return fmt.Errorf("invalid PITR time")
		}
	}
	if r.TargetGTID != "" && !validGTID(r.TargetGTID) {
		return fmt.Errorf("invalid PITR GTID")
	}
	seen := map[string]bool{}
	for _, user := range r.RecoveryUsers {
		if err := user.Validate(); err != nil {
			return err
		}
		if seen[user.Name] {
			return fmt.Errorf("duplicate managed recovery account")
		}
		seen[user.Name] = true
	}
	return nil
}
func validGTID(s string) bool {
	for _, set := range strings.Split(s, ",") {
		parts := strings.Split(strings.TrimSpace(set), ":")
		if len(parts) < 2 || !ValidUUID(parts[0]) {
			return false
		}
		for _, interval := range parts[1:] {
			bounds := strings.Split(interval, "-")
			if len(bounds) > 2 {
				return false
			}
			var prev uint64
			for _, b := range bounds {
				n, err := strconv.ParseUint(b, 10, 64)
				if err != nil || n == 0 || n < prev {
					return false
				}
				prev = n
			}
		}
	}
	return true
}
func (p Proxy) Validate() error {
	if p.Mode != "" && p.Mode != TopologySingle && p.Mode != TopologyGroup && p.Mode != "galera" {
		return fmt.Errorf("invalid proxy mode")
	}
	if len(p.Backends) == 0 || p.MaxConnections < 1 {
		return fmt.Errorf("proxy backends and positive connection limit required")
	}
	for _, b := range p.Backends {
		if err := b.Validate(); err != nil {
			return err
		}
	}
	if p.Monitor.Username != UserMonitor {
		return fmt.Errorf("invalid proxy monitor account")
	}
	if err := p.Monitor.Validate(); err != nil {
		return err
	}
	if err := p.Admin.Validate(); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, u := range p.Users {
		if u.Username == "" || len(u.Username) > 32 || strings.ContainsAny(u.Username+u.Password, "\x00\r\n") || u.Password == "" || !u.RequireTLS || seen[u.Username] || ReservedUser(u.Username) {
			return fmt.Errorf("invalid proxy user")
		}
		seen[u.Username] = true
	}
	return nil
}
func ReservedUser(s string) bool {
	s = strings.ToLower(s)
	return s == "root" || strings.HasPrefix(s, "kk_") || strings.HasPrefix(s, "mysql.")
}
func (s Spec) Validate() error {
	if !memberName.MatchString(s.Member) || s.Ordinal < 0 || (s.Role != RoleData && s.Role != RoleProxy) || (s.Topology != TopologySingle && s.Topology != TopologyGroup && s.Topology != TopologyCluster) || !(version.MatchString(s.Version) || s.Topology == TopologyCluster && galeraVersion.MatchString(s.Version)) {
		return fmt.Errorf("invalid member identity, topology or version")
	}
	ca, _ := pem.Decode([]byte(s.CAPEM))
	if ca == nil || ca.Type != "CERTIFICATE" || s.ServerCert == nil {
		return fmt.Errorf("TLS material required")
	}
	if err := s.ServerCert.Validate(); err != nil {
		return err
	}
	for _, p := range s.Peers {
		if err := p.Validate(); err != nil {
			return err
		}
	}
	if err := ValidateParameters(s.Parameters); err != nil {
		return err
	}
	if s.Role == RoleProxy {
		if s.Proxy == nil || s.GroupReplication != nil || s.Galera != nil || s.Restore != nil || s.Backup != nil || (s.Topology == TopologyCluster) != (s.Proxy.Mode == "galera") {
			return fmt.Errorf("invalid proxy member blocks")
		}
		if s.Proxy.Mode != "" && s.Proxy.Mode != "galera" && s.Proxy.Mode != s.Topology {
			return fmt.Errorf("proxy mode and topology disagree")
		}
		return s.Proxy.Validate()
	}
	serverPrefix := "/opt/karakoram/percona-server-"
	if s.Galera != nil {
		serverPrefix = "/opt/karakoram/percona-xtradb-cluster-"
	}
	if s.Proxy != nil || !strings.HasPrefix(s.ServerDir, serverPrefix) || path.Clean(s.ServerDir) != s.ServerDir || !strings.HasPrefix(s.XtraBackupDir, "/opt/karakoram/percona-xtrabackup-") || path.Clean(s.XtraBackupDir) != s.XtraBackupDir || s.BufferPoolBytes < 5<<20 || s.MaxConnections < 1 {
		return fmt.Errorf("invalid data member settings")
	}
	for name, c := range map[string]Credentials{UserRoot: s.Superuser, UserAgent: s.Agent, UserReplication: s.Replication, UserMonitor: s.Monitor, UserBackup: s.BackupUser} {
		if c.Username != name {
			return fmt.Errorf("system account name must be %s", name)
		}
		if err := c.Validate(); err != nil {
			return err
		}
	}
	if s.Topology == TopologyCluster {
		if s.Galera == nil || s.GroupReplication != nil {
			return fmt.Errorf("cluster requires Galera settings only")
		}
		if err := s.Galera.Validate(); err != nil {
			return err
		}
		trafficCA, _ := pem.Decode([]byte(s.Galera.TrafficCAPEM))
		if bytes.Equal(ca.Bytes, trafficCA.Bytes) {
			return fmt.Errorf("Galera traffic CA must be separate from SQL and agent trust")
		}
		found := false
		for _, peer := range s.Galera.Peers {
			if peer.Name == s.Member {
				found = peer.Port == s.Galera.GaleraPort
			}
		}
		if !found {
			return fmt.Errorf("Galera local peer and port required")
		}
	} else if s.Galera != nil {
		return fmt.Errorf("Galera requires cluster topology")
	}
	if s.Topology == TopologyGroup {
		if s.GroupReplication == nil {
			return fmt.Errorf("group settings required")
		}
		if err := s.GroupReplication.Validate(); err != nil {
			return err
		}
	} else if s.GroupReplication != nil {
		return fmt.Errorf("single cannot carry group settings")
	}
	if s.Backup != nil {
		if err := s.Backup.Repository.Validate(); err != nil {
			return err
		}
		if err := s.Backup.Binlog.Validate(); err != nil {
			return err
		}
	}
	if s.Restore != nil {
		if err := s.Restore.Validate(); err != nil {
			return err
		}
		if name := s.Restore.NewGroupName; name != "" {
			if s.Topology == TopologyCluster {
				if !ValidGaleraClusterName(name) {
					return fmt.Errorf("invalid restored Galera cluster name")
				}
			} else if !ValidUUID(name) {
				return fmt.Errorf("restored MySQL group name must be a UUID")
			}
		}
		if s.Topology == TopologyGroup && s.Restore.NewGroupName == "" {
			return fmt.Errorf("restored group requires a new group name")
		}
	}
	return nil
}
func (r SwitchoverRequest) Validate() error {
	if !ValidUUID(r.TargetUUID) {
		return fmt.Errorf("invalid primary UUID")
	}
	return nil
}
func (r GroupRequest) Validate() error {
	if !ValidUUID(r.GroupName) || r.FormationEpoch == 0 {
		return fmt.Errorf("invalid group identity")
	}
	return nil
}
func (r CloneRequest) Validate() error {
	if !safeObject(r.Receipt) || !version.MatchString(r.Version) {
		return fmt.Errorf("invalid clone request")
	}
	return r.Donor.Validate()
}
func (r BackupRequest) Validate() error {
	if !safeObject(r.Receipt) || !safeObject(r.ObjectPrefix) || r.Type != "full" && r.Type != "incremental" || r.Type == "full" && r.BaseLSN != 0 || r.Type == "incremental" && r.BaseLSN == 0 {
		return fmt.Errorf("invalid backup request")
	}
	return r.Repository.Validate()
}
func (r RestoreRequest) Validate() error {
	if r.PlanDigest != "" && !ValidDigest(r.PlanDigest) {
		return fmt.Errorf("invalid restore plan digest")
	}
	if !safeObject(r.Receipt) {
		return fmt.Errorf("invalid restore receipt")
	}
	return r.Restore.Validate()
}

// RestartRequiredParameter classifies the startup-only subset of the allowlist.
func RestartRequiredParameter(name string) bool {
	switch name {
	case "innodb_buffer_pool_instances", "table_open_cache_instances":
		return true
	default:
		return false
	}
}

// ValidateParameters is shared by admission and the member to keep the my.cnf boundary closed.
func ValidateParameters(parameters map[string]string) error {
	for k, v := range parameters {
		if v == "" || strings.ContainsAny(v, "\x00\r\n#;'") {
			return fmt.Errorf("invalid MySQL parameter %s", k)
		}
		var lo, hi int64
		switch k {
		case "innodb_buffer_pool_instances", "table_open_cache_instances":
			lo, hi = 1, 64
		case "max_connections":
			lo, hi = 1, 100000
		case "innodb_buffer_pool_size":
			lo, hi = 5<<20, 1<<50
		case "innodb_io_capacity", "innodb_io_capacity_max":
			lo, hi = 100, 10000000
		case "innodb_lock_wait_timeout":
			lo, hi = 1, 1073741824
		case "lock_wait_timeout", "wait_timeout", "interactive_timeout":
			lo, hi = 1, 31536000
		case "long_query_time":
			n, err := strconv.ParseFloat(v, 64)
			if err != nil || n < 0 || n > 31536000 || strings.ContainsAny(v, "NaInf") {
				return fmt.Errorf("invalid MySQL parameter %s", k)
			}
			continue
		case "slow_query_log", "log_queries_not_using_indexes", "innodb_print_all_deadlocks":
			if v != "ON" && v != "OFF" && v != "0" && v != "1" {
				return fmt.Errorf("invalid boolean parameter %s", k)
			}
			continue
		case "transaction_isolation":
			if v != "READ-UNCOMMITTED" && v != "READ-COMMITTED" && v != "REPEATABLE-READ" && v != "SERIALIZABLE" {
				return fmt.Errorf("invalid isolation")
			}
			continue
		case "max_allowed_packet":
			lo, hi = 1024, 1<<30
		case "sort_buffer_size", "join_buffer_size", "read_buffer_size", "read_rnd_buffer_size":
			lo, hi = 128, 1<<30
		case "tmp_table_size", "max_heap_table_size":
			lo, hi = 1024, 1<<40
		case "table_open_cache", "thread_cache_size":
			lo, hi = 1, 1000000
		default:
			return fmt.Errorf("MySQL parameter %s is protected or unsupported", k)
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < lo || n > hi {
			return fmt.Errorf("invalid MySQL parameter %s", k)
		}
	}
	return nil
}

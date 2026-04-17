package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultPort               = "8088"
	envPort                   = "PORT"
	defaultPythonExecutable   = "python3"
	defaultPythonEntry        = "main.py"
	defaultPipelineTimeout    = 150 * time.Second
	defaultIngestionInterval  = 60 * time.Minute
	defaultDBPort             = "5432"
	defaultDBUser             = "radar"
	defaultDBPassword         = "radar_password"
	defaultDBName             = "deep_signal_radar"
	envProjectRoot            = "DSR_PROJECT_ROOT"
	envPythonExecutable       = "PYTHON_EXECUTABLE"
	envPythonExec             = "PYTHON_EXEC"
	envPipelineTimeoutSeconds = "PYTHON_PIPELINE_TIMEOUT_SECONDS"
	envIngestionIntervalMins  = "INGEST_INTERVAL_MINUTES"
	envDatabaseURL            = "DATABASE_URL"
	envPostgresHost           = "POSTGRES_HOST"
	envPostgresPort           = "POSTGRES_PORT"
	envPostgresUser           = "POSTGRES_USER"
	envPostgresPassword       = "POSTGRES_PASSWORD"
	envPostgresDB             = "POSTGRES_DB"
	maxErrorDetailLength      = 2048
	maxLogDetailLength        = 512
)

const upsertSignalSQL = `
INSERT INTO signals (title, url, signal_score, reason, summary)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (url) DO UPDATE SET
    title = EXCLUDED.title,
    signal_score = EXCLUDED.signal_score,
    reason = EXCLUDED.reason,
    summary = EXCLUDED.summary,
    created_at = NOW();
`

const selectLatestSignalsSQL = `
SELECT id, title, url, signal_score, reason, summary, created_at
FROM signals
ORDER BY created_at DESC
LIMIT 50;
`

type app struct {
	logger            *slog.Logger
	db                *pgxpool.Pool
	projectRoot       string
	pythonExecutable  string
	pipelineTimeout   time.Duration
	ingestionInterval time.Duration
}

// errorResponse is the JSON envelope for HTTP error responses.
type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
}

// signalRecord is the storage shape for extracted signals.
type signalRecord struct {
	ID          int       `json:"id,omitempty"`
	Title       string    `json:"title"`
	URL         string    `json:"url"`
	SignalScore int       `json:"signal_score"`
	Reason      string    `json:"reason"`
	Summary     string    `json:"summary"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
}

// ingestResponse is the JSON envelope for manual ingestion responses.
type ingestResponse struct {
	Trigger  string         `json:"trigger"`
	Ingested int            `json:"ingested"`
	Signals  []signalRecord `json:"signals"`
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	projectRoot, err := resolveProjectRoot()
	if err != nil {
		logger.Error("failed to resolve project root", slog.String("err", err.Error()))
		os.Exit(1)
	}

	entryPath := filepath.Join(projectRoot, defaultPythonEntry)
	if _, err := os.Stat(entryPath); err != nil {
		logger.Error(
			"python entry file not found",
			slog.String("path", entryPath),
			slog.String("err", err.Error()),
		)
		os.Exit(1)
	}

	pythonExec, pythonSource := resolvePythonExecutable(projectRoot, logger)
	if pythonExec == "" {
		logger.Error("failed to resolve a Python executable")
		os.Exit(1)
	}

	databaseURL := resolveDatabaseURL()
	dbPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		logger.Error("failed to create database connection pool", slog.String("err", err.Error()))
		os.Exit(1)
	}
	defer dbPool.Close()

	if err := dbPool.Ping(ctx); err != nil {
		logger.Error("failed to connect to database", slog.String("err", err.Error()))
		os.Exit(1)
	}

	pipelineTimeout := resolvePipelineTimeout(logger)
	ingestionInterval := resolveIngestionInterval(logger)
	rawPortEnv := strings.TrimSpace(os.Getenv(envPort))
	listenAddr := resolveListenAddr(logger, rawPortEnv)

	application := &app{
		logger:            logger,
		db:                dbPool,
		projectRoot:       projectRoot,
		pythonExecutable:  pythonExec,
		pipelineTimeout:   pipelineTimeout,
		ingestionInterval: ingestionInterval,
	}

	go application.startIngestionLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/signals", application.handleSignals)
	mux.HandleFunc("POST /api/v1/ingest", application.handleIngest)

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           withAccessLog(logger, listenAddr, mux),
		ReadHeaderTimeout: 10 * time.Second,
	}

	logger.Info(
		"starting HTTP server",
		slog.String("listen_addr", listenAddr),
		slog.String(envPort, rawPortEnv),
		slog.String("project_root", projectRoot),
		slog.String("python_executable", pythonExec),
		slog.String("python_executable_source", pythonSource),
		slog.Duration("pipeline_timeout", pipelineTimeout),
		slog.Duration("ingestion_interval", ingestionInterval),
	)

	errCh := make(chan error, 1)
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-errCh:
		if err != nil {
			logger.Error(
				"server terminated unexpectedly",
				slog.String("listen_addr", listenAddr),
				slog.String("err", err.Error()),
			)
			os.Exit(1)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("failed to gracefully shutdown server", slog.String("err", err.Error()))
		os.Exit(1)
	}
}

func resolveProjectRoot() (string, error) {
	if v := strings.TrimSpace(os.Getenv(envProjectRoot)); v != "" {
		abs, err := filepath.Abs(v)
		if err != nil {
			return "", fmt.Errorf("resolve %s: %w", envProjectRoot, err)
		}
		return abs, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("get working directory: %w", err)
	}
	abs, err := filepath.Abs(cwd)
	if err != nil {
		return "", fmt.Errorf("resolve working directory: %w", err)
	}
	return abs, nil
}

func resolvePythonExecutable(projectRoot string, logger *slog.Logger) (string, string) {
	venvPython := filepath.Join(projectRoot, ".venv", "bin", "python")
	if info, err := os.Stat(venvPython); err == nil && !info.IsDir() {
		if abs, absErr := filepath.Abs(venvPython); absErr == nil {
			logger.Info(
				"resolved Python executable",
				slog.String("python_executable", abs),
				slog.String("source", "local_virtual_environment"),
			)
			return abs, "local_virtual_environment"
		}
		logger.Info(
			"resolved Python executable",
			slog.String("python_executable", venvPython),
			slog.String("source", "local_virtual_environment"),
		)
		return venvPython, "local_virtual_environment"
	}

	if envExec := resolvePythonExecutableFromEnv(); envExec != "" {
		if resolved, err := normalizePythonExecutablePath(projectRoot, envExec); err == nil {
			logger.Info(
				"resolved Python executable",
				slog.String("python_executable", resolved),
				slog.String("source", "environment"),
			)
			return resolved, "environment"
		}
		logger.Warn(
			"invalid Python executable from environment, falling back to default",
			slog.String("value", envExec),
			slog.String("primary_env", envPythonExecutable),
			slog.String("secondary_env", envPythonExec),
		)
	}

	if resolved, err := normalizePythonExecutablePath(projectRoot, defaultPythonExecutable); err == nil {
		logger.Info(
			"resolved Python executable",
			slog.String("python_executable", resolved),
			slog.String("source", "default"),
		)
		return resolved, "default"
	}

	logger.Warn(
		"default Python executable not found in PATH, using fallback command name",
		slog.String("python_executable", defaultPythonExecutable),
	)
	return defaultPythonExecutable, "default_fallback"
}

func resolvePythonExecutableFromEnv() string {
	if v := strings.TrimSpace(os.Getenv(envPythonExecutable)); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv(envPythonExec))
}

func normalizePythonExecutablePath(projectRoot, executable string) (string, error) {
	if executable == "" {
		return "", fmt.Errorf("python executable is empty")
	}

	if strings.ContainsRune(executable, os.PathSeparator) {
		candidate := executable
		if !filepath.IsAbs(candidate) {
			candidate = filepath.Join(projectRoot, candidate)
		}
		if _, err := os.Stat(candidate); err != nil {
			return "", fmt.Errorf("stat executable path: %w", err)
		}
		abs, err := filepath.Abs(candidate)
		if err != nil {
			return "", fmt.Errorf("resolve executable path: %w", err)
		}
		return abs, nil
	}

	resolved, err := exec.LookPath(executable)
	if err != nil {
		return "", fmt.Errorf("look up executable in PATH: %w", err)
	}
	abs, err := filepath.Abs(resolved)
	if err != nil {
		return resolved, nil
	}
	return abs, nil
}

func resolveDatabaseURL() string {
	if v := strings.TrimSpace(os.Getenv(envDatabaseURL)); v != "" {
		return v
	}

	host := envOrDefault(envPostgresHost, "localhost")
	port := envOrDefault(envPostgresPort, defaultDBPort)
	user := envOrDefault(envPostgresUser, defaultDBUser)
	password := envOrDefault(envPostgresPassword, defaultDBPassword)
	dbName := envOrDefault(envPostgresDB, defaultDBName)

	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

// resolveListenAddr returns the TCP address for http.Server.Addr.
// If rawPortEnv is empty, it binds to all interfaces on defaultPort (8088).
// If set, PORT may be a numeric port, a ":port" form, or a "host:port" pair.
func resolveListenAddr(logger *slog.Logger, rawPortEnv string) string {
	fallback := ":" + defaultPort
	if rawPortEnv == "" {
		return fallback
	}
	if strings.HasPrefix(rawPortEnv, ":") {
		port := strings.TrimPrefix(rawPortEnv, ":")
		if p, ok := parsePortNumber(port); ok {
			return ":" + strconv.Itoa(p)
		}
		logger.Warn(
			"invalid PORT value after leading colon, using default listen address",
			slog.String("env", envPort),
			slog.String("value", rawPortEnv),
			slog.String("fallback_listen_addr", fallback),
		)
		return fallback
	}
	if strings.Contains(rawPortEnv, ":") {
		host, portStr, err := net.SplitHostPort(rawPortEnv)
		if err != nil {
			logger.Warn(
				"invalid host:port in PORT, using default listen address",
				slog.String("env", envPort),
				slog.String("value", rawPortEnv),
				slog.String("fallback_listen_addr", fallback),
			)
			return fallback
		}
		if _, ok := parsePortNumber(portStr); !ok {
			logger.Warn(
				"invalid port number in PORT host:port, using default listen address",
				slog.String("env", envPort),
				slog.String("value", rawPortEnv),
				slog.String("fallback_listen_addr", fallback),
			)
			return fallback
		}
		return net.JoinHostPort(host, portStr)
	}
	if p, ok := parsePortNumber(rawPortEnv); ok {
		return ":" + strconv.Itoa(p)
	}
	logger.Warn(
		"invalid PORT value, using default listen address",
		slog.String("env", envPort),
		slog.String("value", rawPortEnv),
		slog.String("fallback_listen_addr", fallback),
	)
	return fallback
}

func parsePortNumber(s string) (int, bool) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 65535 {
		return 0, false
	}
	return n, true
}

func resolvePipelineTimeout(logger *slog.Logger) time.Duration {
	raw := strings.TrimSpace(os.Getenv(envPipelineTimeoutSeconds))
	if raw == "" {
		return defaultPipelineTimeout
	}
	sec, err := parsePositiveDurationSeconds(raw)
	if err != nil {
		logger.Warn(
			"invalid pipeline timeout env, using default",
			slog.String("env", envPipelineTimeoutSeconds),
			slog.String("value", raw),
			slog.String("err", err.Error()),
			slog.Duration("default", defaultPipelineTimeout),
		)
		return defaultPipelineTimeout
	}
	return sec
}

func resolveIngestionInterval(logger *slog.Logger) time.Duration {
	raw := strings.TrimSpace(os.Getenv(envIngestionIntervalMins))
	if raw == "" {
		return defaultIngestionInterval
	}

	minutes, err := strconv.Atoi(raw)
	if err != nil || minutes <= 0 {
		logger.Warn(
			"invalid ingestion interval env, using default",
			slog.String("env", envIngestionIntervalMins),
			slog.String("value", raw),
			slog.Duration("default", defaultIngestionInterval),
		)
		return defaultIngestionInterval
	}
	return time.Duration(minutes) * time.Minute
}

func parsePositiveDurationSeconds(s string) (time.Duration, error) {
	sec, err := strconv.ParseFloat(s, 64)
	if err != nil || sec <= 0 {
		return 0, fmt.Errorf("expected positive number of seconds")
	}
	return time.Duration(sec * float64(time.Second)), nil
}

func withAccessLog(logger *slog.Logger, listenAddr string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		logger.Info(
			"http_request",
			slog.String("listen_addr", listenAddr),
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Duration("duration", time.Since(start)),
		)
	})
}

func (a *app) startIngestionLoop(ctx context.Context) {
	a.ingestSignalsOnce(ctx, "startup")

	ticker := time.NewTicker(a.ingestionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			a.logger.Info("stopping ingestion loop")
			return
		case <-ticker.C:
			a.ingestSignalsOnce(ctx, "ticker")
		}
	}
}

func (a *app) ingestSignalsOnce(parent context.Context, trigger string) {
	if _, err := a.ingestSignals(parent, trigger); err != nil {
		return
	}
}

func (a *app) ingestSignals(parent context.Context, trigger string) ([]signalRecord, error) {
	ctx, cancel := context.WithTimeout(parent, a.pipelineTimeout)
	defer cancel()

	stdout, stderr, runErr := runPythonPipeline(ctx, a.projectRoot, a.pythonExecutable)
	if ctx.Err() == context.DeadlineExceeded {
		a.logger.Error(
			"python_pipeline_timeout",
			slog.String("trigger", trigger),
			slog.Duration("timeout", a.pipelineTimeout),
			slog.String("stderr_tail", truncateString(stderr, maxLogDetailLength)),
		)
		return nil, ctx.Err()
	}

	if runErr != nil {
		a.logger.Error(
			"python_pipeline_failed",
			slog.String("trigger", trigger),
			slog.String("err", runErr.Error()),
			slog.String("stderr_tail", truncateString(stderr, maxLogDetailLength)),
		)
		return nil, runErr
	}

	signals, err := parsePipelineOutput(stdout)
	if err != nil {
		a.logger.Error(
			"python_pipeline_invalid_output",
			slog.String("trigger", trigger),
			slog.String("err", err.Error()),
			slog.String("stdout_tail", truncateString(stdout, maxLogDetailLength)),
		)
		return nil, err
	}

	a.logger.Info(
		"extracted signals from python",
		slog.String("trigger", trigger),
		slog.Int("count", len(signals)),
	)

	if len(signals) == 0 {
		a.logger.Info("ingestion completed with no signals", slog.String("trigger", trigger))
		return []signalRecord{}, nil
	}

	if err := a.upsertSignals(parent, signals); err != nil {
		a.logger.Error(
			"failed to upsert signals",
			slog.String("trigger", trigger),
			slog.Int("count", len(signals)),
			slog.String("err", err.Error()),
		)
		return nil, err
	}

	a.logger.Info(
		"upserted signals to DB",
		slog.String("trigger", trigger),
		slog.Int("count", len(signals)),
	)

	a.logger.Info(
		"signals ingested successfully",
		slog.String("trigger", trigger),
		slog.Int("count", len(signals)),
	)
	return signals, nil
}

func parsePipelineOutput(stdout string) ([]signalRecord, error) {
	out := bytes.TrimSpace([]byte(stdout))
	if len(out) == 0 {
		return []signalRecord{}, nil
	}

	if !json.Valid(out) {
		return nil, fmt.Errorf("pipeline output is not valid JSON")
	}

	var signals []signalRecord
	if err := json.Unmarshal(out, &signals); err != nil {
		return nil, fmt.Errorf("pipeline output is not a valid signal array: %w", err)
	}
	return signals, nil
}

func (a *app) upsertSignals(ctx context.Context, signals []signalRecord) error {
	tx, err := a.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	for _, sig := range signals {
		if _, err := tx.Exec(
			ctx,
			upsertSignalSQL,
			sig.Title,
			sig.URL,
			sig.SignalScore,
			sig.Reason,
			sig.Summary,
		); err != nil {
			return fmt.Errorf("upsert url %q: %w", sig.URL, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (a *app) handleSignals(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	rows, err := a.db.Query(ctx, selectLatestSignalsSQL)
	if err != nil {
		a.logger.Error("failed to query signals", slog.String("err", err.Error()))
		writeJSONError(
			w,
			http.StatusInternalServerError,
			"database_query_failed",
			"Failed to fetch signals from database.",
			truncateString(err.Error(), maxErrorDetailLength),
		)
		return
	}
	defer rows.Close()

	results := make([]signalRecord, 0, 50)
	for rows.Next() {
		var row signalRecord
		if err := rows.Scan(
			&row.ID,
			&row.Title,
			&row.URL,
			&row.SignalScore,
			&row.Reason,
			&row.Summary,
			&row.CreatedAt,
		); err != nil {
			a.logger.Error("failed to scan signal row", slog.String("err", err.Error()))
			writeJSONError(
				w,
				http.StatusInternalServerError,
				"database_scan_failed",
				"Failed to parse signals from database.",
				truncateString(err.Error(), maxErrorDetailLength),
			)
			return
		}
		results = append(results, row)
	}

	if err := rows.Err(); err != nil {
		a.logger.Error("database row iteration failed", slog.String("err", err.Error()))
		writeJSONError(
			w,
			http.StatusInternalServerError,
			"database_iteration_failed",
			"Failed while reading signals from database.",
			truncateString(err.Error(), maxErrorDetailLength),
		)
		return
	}

	writeJSON(w, http.StatusOK, results)
}

func (a *app) handleIngest(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	trigger := "manual_api"
	start := time.Now()

	a.logger.Info(
		"manual ingestion requested",
		slog.String("trigger", trigger),
		slog.String("remote_addr", r.RemoteAddr),
	)

	signals, err := a.ingestSignals(ctx, trigger)
	if err != nil {
		detail := composeFailureDetail(err, "")
		a.logger.Error(
			"manual ingestion failed",
			slog.String("trigger", trigger),
			slog.String("err", err.Error()),
			slog.Duration("duration", time.Since(start)),
		)
		writeJSONError(
			w,
			http.StatusInternalServerError,
			"ingestion_failed",
			"Manual ingestion failed.",
			detail,
		)
		return
	}

	a.logger.Info(
		"manual ingestion completed",
		slog.String("trigger", trigger),
		slog.Int("ingested", len(signals)),
		slog.Duration("duration", time.Since(start)),
	)

	writeJSON(w, http.StatusOK, ingestResponse{
		Trigger:  trigger,
		Ingested: len(signals),
		Signals:  signals,
	})
}

func runPythonPipeline(
	ctx context.Context,
	projectRoot string,
	pythonExecutable string,
) (stdout string, stderr string, err error) {
	cmd := exec.CommandContext(ctx, pythonExecutable, defaultPythonEntry)
	cmd.Dir = projectRoot

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	runErr := cmd.Run()
	return stdoutBuf.String(), stderrBuf.String(), runErr
}

func composeFailureDetail(runErr error, stderr string) string {
	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		return truncateString(
			fmt.Sprintf("exit_code=%d; stderr=%s", exitErr.ExitCode(), strings.TrimSpace(stderr)),
			maxErrorDetailLength,
		)
	}
	return truncateString(
		fmt.Sprintf("%s; stderr=%s", runErr.Error(), strings.TrimSpace(stderr)),
		maxErrorDetailLength,
	)
}

func writeJSONError(w http.ResponseWriter, status int, code, message, detail string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	payload := errorResponse{
		Error:   code,
		Message: message,
		Detail:  detail,
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(true)
	_ = enc.Encode(payload)
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(true)
	_ = enc.Encode(payload)
}

func truncateString(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

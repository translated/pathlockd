<?php
declare(strict_types=1);

pathlockd_polling_demo($argv);

function pathlockd_polling_demo(array $argv): void
{
    $base = getenv('PATHLOCKD_WEB_URL') ?: 'https://localhost:8443';
    $client = new PathlockdClient($base, verifyTls: false);

    $client->health();
    echo "connected to pathlockd at $base\n";

    $owner = 'php-worker-1';
    $path = 'mutex:/critical-section';
    $ttlMs = 30000;
    $queueTtlMs = 60000;

    $client->releaseAll(['ownerId' => $owner]);

    $fence = acquire_with_polling($client, $owner, $path, $ttlMs, $queueTtlMs);
    if ($fence === null) {
        fwrite(STDERR, "acquire failed — aborting\n");
        exit(1);
    }

    echo "[$owner] holding lock (fence=$fence)\n";

    $lastRenew = time();
    $renewInterval = (int)($ttlMs / 1000 / 3);

    for ($i = 1; $i <= 3; $i++) {
        if (time() - $lastRenew >= $renewInterval) {
            $r = $client->renew(['ownerId' => $owner, 'ttlMs' => $ttlMs]);
            if ($r['status'] !== 0) {
                fwrite(STDERR, "[$owner] renew LOST: " . json_encode($r) . "\n");
                exit(1);
            }
            $lastRenew = time();
            echo "[$owner] renewed\n";
        }

        if (!$client->isOwnerAlive(['ownerId' => $owner])['alive']) {
            fwrite(STDERR, "[$owner] preempted (isOwnerAlive=false) — aborting\n");
            exit(1);
        }

        $assert = $client->assertFencing([
            'ownerId' => $owner,
            'fencingToken' => $fence,
            'paths' => [$path],
        ]);
        if ($assert['status'] !== 0) {
            fwrite(STDERR, "[$owner] fencing failed on {$assert['path']} "
                . "({$assert['reason']}) — aborting write\n");
            exit(1);
        }
        echo "[$owner] write #$i (fence OK)\n";
        sleep(1);
    }

    $client->release([
        'ownerId' => $owner,
        'requests' => [['path' => $path, 'mode' => 0]],
    ]);
    echo "[$owner] released. polling demo complete.\n";
}

function acquire_with_polling(
    PathlockdClient $client,
    string $owner,
    string $path,
    int $ttlMs,
    int $queueTtlMs,
): ?int {
    echo "[$owner] acquiring $path ...\n";
    $resp = $client->acquire([
        'ownerId' => $owner,
        'ttlMs' => $ttlMs,
        'requests' => [['path' => $path, 'mode' => 0]],
        'queueTtlMs' => $queueTtlMs,
    ]);

    if ($resp['status'] === 0) {
        return $resp['fencingToken'];
    }

    if ($resp['status'] !== 3) {
        fwrite(STDERR, "[$owner] acquire failed: " . json_encode($resp) . "\n");
        return null;
    }

    $blocker = $resp['owner'] ?? '?';
    $reason = $resp['reason'] ?? '?';
    echo "[$owner] QUEUED behind $blocker ($reason); polling listOwnerLocks ...\n";

    $deadline = time() + (int)($queueTtlMs / 1000);
    while (time() < $deadline) {
        usleep(500_000);

        $alive = $client->isOwnerAlive(['ownerId' => $owner]);
        if (!$alive['alive']) {
            fwrite(STDERR, "[$owner] preempted while queued — aborting\n");
            return null;
        }

        $locks = $client->listOwnerLocks(['ownerId' => $owner]);
        foreach ($locks['locks'] as $lock) {
            if ($lock['path'] === $path) {
                echo "[$owner] path appears in held set — re-issuing acquire\n";
                $resp = $client->acquire([
                    'ownerId' => $owner,
                    'ttlMs' => $ttlMs,
                    'requests' => [['path' => $path, 'mode' => 0]],
                ]);
                if ($resp['status'] === 0) {
                    return $resp['fencingToken'];
                }
                fwrite(STDERR, "[$owner] re-acquire failed: "
                    . json_encode($resp) . "\n");
                return null;
            }
        }
    }

    fwrite(STDERR, "[$owner] queue TTL lapsed — abandoning\n");
    return null;
}

class PathlockdClient
{
    private string $baseUrl;
    private bool $verifyTls;

    public function __construct(string $baseUrl, bool $verifyTls = true)
    {
        $this->baseUrl = rtrim($baseUrl, '/');
        $this->verifyTls = $verifyTls;
    }

    public function health(): array
    {
        return $this->request('GET', '/v1/health');
    }

    public function acquire(array $body): array
    {
        return $this->request('POST', '/v1/acquire', $body);
    }

    public function release(array $body): array
    {
        return $this->request('POST', '/v1/release', $body);
    }

    public function releaseAll(array $body): array
    {
        return $this->request('POST', '/v1/releaseAll', $body);
    }

    public function renew(array $body): array
    {
        return $this->request('POST', '/v1/renew', $body);
    }

    public function assertFencing(array $body): array
    {
        return $this->request('POST', '/v1/assertFencing', $body);
    }

    public function listOwnerLocks(array $body): array
    {
        return $this->request('POST', '/v1/listOwnerLocks', $body);
    }

    public function inspectPath(string $path): array
    {
        return $this->request('POST', '/v1/inspectPath', ['path' => $path]);
    }

    public function isOwnerAlive(array $body): array
    {
        return $this->request('POST', '/v1/isOwnerAlive', $body);
    }

    private function request(string $method, string $path, ?array $body = null): array
    {
        $ch = curl_init($this->baseUrl . $path);
        curl_setopt_array($ch, [
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CUSTOMREQUEST => $method,
            CURLOPT_TIMEOUT => 30,
        ]);
        if (!$this->verifyTls) {
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
        }
        if ($body !== null) {
            curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($body));
            curl_setopt($ch, CURLOPT_HTTPHEADER, ['Content-Type: application/json']);
        }

        $raw = curl_exec($ch);
        $status = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $err = curl_error($ch);
        curl_close($ch);

        if ($err !== '') {
            throw new RuntimeException("cURL error: $err");
        }

        $data = json_decode($raw, true) ?? [];
        if ($status !== 200) {
            $code = $data['error']['code'] ?? 'UNKNOWN';
            $msg = $data['error']['message'] ?? '';
            throw new PathlockdError($code, $msg, $status);
        }
        return $data;
    }
}

class PathlockdError extends Exception
{
    public string $errorCode;
    public int $httpStatus;

    public function __construct(string $code, string $message, int $httpStatus)
    {
        parent::__construct("$code: $message");
        $this->errorCode = $code;
        $this->httpStatus = $httpStatus;
    }
}

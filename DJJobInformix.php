<?php

function array_change_key_case_recursive(&$array, $case=CASE_LOWER, $flag_rec=false)
{
    $array = array_change_key_case($array, $case);
    if ( $flag_rec ) {
        foreach ($array as $key => $value) {
            if ( is_array($value) ) {
                array_change_key_case_recursive($array[$key], $case, true);
            }
        }
    }
}

trait DJBaseInformix {

    //http://php.net/manual/es/language.oop5.traits.php#language.oop5.traits.precedence
    public static function runQuery($sql, $params = array()) {

        $ret = parent::runQuery($sql, $params);
        //informix devuelve las claves del ResultSet en mayuscula, acá las paso a minuscula.
        array_change_key_case_recursive($ret, CASE_LOWER, true);
        return $ret;

    }
}

class DJWorkerInformix extends DJWorker {

    use DJBaseInformix;

    /**
     * Returns a new job ordered by most recent first
     * why this?
     *     run newest first, some jobs get left behind
     *     run oldest first, all jobs get left behind
     * @return DJJob
     */
    public function getNewJob() {
        # we can grab a locked job if we own the lock
        $rs = $this->runQuery("
            SELECT FIRST 10 id, created_at
            FROM   jobs
            WHERE  queue = ?
            AND    (run_at IS NULL OR CURRENT >= run_at)
            AND    (locked_at IS NULL OR locked_by = ?)
            AND    failed_at IS NULL
            AND    attempts < ?
            ORDER BY created_at DESC
        ", array($this->queue, $this->name, $this->max_attempts));

        // randomly order the 10 to prevent lock contention among workers
        shuffle($rs);

        foreach ($rs as $r) {
            $job = new DJJobInformix($this->name, $r["id"], array(
                "max_attempts" => $this->max_attempts
            ));
            if ($job->acquireLock()) return $job;
        }

        return false;
    }

}

class DJJobInformix extends DJJob{

    use DJBaseInformix;

    public function acquireLock() {
        $this->log("[JOB] attempting to acquire lock for job::{$this->job_id} on {$this->worker_name}", self::INFO);

        $lock = $this->runUpdate("
            UPDATE jobs
            SET    locked_at = CURRENT, locked_by = ?
            WHERE  id = ? AND (locked_at IS NULL OR locked_by = ?) AND failed_at IS NULL
        ", array($this->worker_name, $this->job_id, $this->worker_name));

        if (!$lock) {
            $this->log("[JOB] failed to acquire lock for job::{$this->job_id}", self::INFO);
            return false;
        }

        return true;
    }

    public function finishWithError($error, $handler = null) {
        $con = self::getConnection();
        $this->runUpdate("
            UPDATE jobs
            SET attempts = attempts + 1,
                failed_at = CASE WHEN (attempts + 1) >= ".$con->quote($this->max_attempts)." THEN CURRENT ELSE NULL END,
                error = CASE WHEN (attempts + 1) >= ".$con->quote($this->max_attempts)." THEN ".$con->quote($error)." ELSE NULL END
            WHERE id = ".$con->quote($this->job_id)
        );
        $this->log("[JOB] failure in job::{$this->job_id}", self::ERROR);
        $this->releaseLock();

        if ($handler && ($this->getAttempts() == $this->max_attempts) && method_exists($handler, '_onDjjobRetryError')) {
            $handler->_onDjjobRetryError($error);
        }
    }

    public function retryLater($delay) {
        $this->runUpdate("
            UPDATE jobs
            SET run_at = EXTEND(CURRENT, YEAR TO SECOND) + ? UNITS SECOND,
                attempts = attempts + 1
            WHERE id = ?",
            array(
                $delay,
                $this->job_id
            )
        );
        $this->releaseLock();
    }

    /*public function getHandler() {
        $stmt = self::getConnection()->prepare("SELECT handler FROM jobs WHERE id = ?");
        $stmt->execute(array($this->job_id));
        $stmt->bindColumn(1, $handler, PDO::PARAM_LOB);

        if ($stmt->fetch(PDO::FETCH_BOUND)) {
            return unserialize(base64_decode(stream_get_contents($handler)));
        }
        else{
            return false;
        }
    }*/

    public static function enqueue($handler, $queue = "default", $run_at = null) {

        if(!is_null($run_at)){
            $run_at.= " 00:00:00";
        }

        $affected = self::runUpdate(
            "INSERT INTO jobs (handler, queue, run_at, created_at) VALUES(?, ?, ?, CURRENT)",
            array(base64_encode(serialize($handler)), (string) $queue, $run_at)
        );

        if ($affected < 1) {
            self::log("[JOB] failed to enqueue new job", self::ERROR);
            return false;
        }

        return true;
    }

    public static function bulkEnqueue($handlers, $queue = "default", $run_at = null) {

        if(!is_null($run_at)){
            $run_at.= " 00:00:00";
        }

        $sql = "INSERT INTO jobs (handler, queue, run_at, created_at) VALUES";
        $sql .= implode(",", array_fill(0, count($handlers), "(?, ?, ?, CURRENT)"));

        $parameters = array();
        foreach ($handlers as $handler) {
            $parameters []= base64_encode(serialize($handler));
            $parameters []= (string) $queue;
            $parameters []= $run_at;
        }
        $affected = self::runUpdate($sql, $parameters);

        if ($affected < 1) {
            self::log("[JOB] failed to enqueue new jobs", self::ERROR);
            return false;
        }

        if ($affected != count($handlers))
            self::log("[JOB] failed to enqueue some new jobs", self::ERROR);

        return true;
    }
}
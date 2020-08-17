//  Copyright (c) 2018-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/error_handler.h"
#include "db/db_impl/db_impl.h"
#include "db/event_helpers.h"
#include "file/sst_file_manager_impl.h"

namespace ROCKSDB_NAMESPACE {

// Maps to help decide the severity of an error based on the
// BackgroundErrorReason, Code, SubCode and whether db_options.paranoid_checks
// is set or not. There are 3 maps, going from most specific to least specific
// (i.e from all 4 fields in a tuple to only the BackgroundErrorReason and
// paranoid_checks). The less specific map serves as a catch all in case we miss
// a specific error code or subcode.
std::map<std::tuple<BackgroundErrorReason, Status::Code, Status::SubCode, bool>,
         Status::Severity>
    ErrorSeverityMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kSoftError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, Status::SubCode::kSpaceLimit,
                         true),
         Status::Severity::kHardError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kNoSpace, true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kNoSpace, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         Status::SubCode::kSpaceLimit, true),
         Status::Severity::kHardError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kHardError},
        // Errors during MANIFEST write
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         true),
         Status::Severity::kHardError},
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, Status::SubCode::kNoSpace,
                         false),
         Status::Severity::kHardError},
};

std::map<std::tuple<BackgroundErrorReason, Status::Code, bool>,
         Status::Severity>
    DefaultErrorSeverityMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kCompaction,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kFlush,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlush, Status::Code::kIOError,
                         false),
         Status::Severity::kNoError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kCorruption, true),
         Status::Severity::kUnrecoverableError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kCorruption, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback,
                         Status::Code::kIOError, false),
         Status::Severity::kNoError},
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kManifestWrite,
                         Status::Code::kIOError, false),
         Status::Severity::kFatalError},
};

std::map<std::tuple<BackgroundErrorReason, bool>, Status::Severity>
    DefaultReasonMap = {
        // Errors during BG compaction
        {std::make_tuple(BackgroundErrorReason::kCompaction, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kCompaction, false),
         Status::Severity::kNoError},
        // Errors during BG flush
        {std::make_tuple(BackgroundErrorReason::kFlush, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kFlush, false),
         Status::Severity::kNoError},
        // Errors during Write
        {std::make_tuple(BackgroundErrorReason::kWriteCallback, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kWriteCallback, false),
         Status::Severity::kFatalError},
        // Errors during Memtable update
        {std::make_tuple(BackgroundErrorReason::kMemTable, true),
         Status::Severity::kFatalError},
        {std::make_tuple(BackgroundErrorReason::kMemTable, false),
         Status::Severity::kFatalError},
};

void ErrorHandler::CancelErrorRecovery() {
#ifndef ROCKSDB_LITE
  db_mutex_->AssertHeld();

  // We'll release the lock before calling sfm, so make sure no new
  // recovery gets scheduled at that point
  auto_recovery_ = false;
  SstFileManagerImpl* sfm = reinterpret_cast<SstFileManagerImpl*>(
      db_options_.sst_file_manager.get());
  if (sfm) {
    // This may or may not cancel a pending recovery
    db_mutex_->Unlock();
    bool cancelled = sfm->CancelErrorRecovery(this);
    db_mutex_->Lock();
    if (cancelled) {
      recovery_in_prog_ = false;
    }
  }

  // If auto recovery is also runing to resume from the retryable error,
  // we should wait and end the auto recovery.
  EndAutoRecovery();
#endif
}

// This is the main function for looking at an error during a background
// operation and deciding the severity, and error recovery strategy. The high
// level algorithm is as follows -
// 1. Classify the severity of the error based on the ErrorSeverityMap,
//    DefaultErrorSeverityMap and DefaultReasonMap defined earlier
// 2. Call a Status code specific override function to adjust the severity
//    if needed. The reason for this is our ability to recover may depend on
//    the exact options enabled in DBOptions
// 3. Determine if auto recovery is possible. A listener notification callback
//    is called, which can disable the auto recovery even if we decide its
//    feasible
// 4. For Status::NoSpace() errors, rely on SstFileManagerImpl to control
//    the actual recovery. If no sst file manager is specified in DBOptions,
//    a default one is allocated during DB::Open(), so there will always be
//    one.
// This can also get called as part of a recovery operation. In that case, we
// also track the error separately in recovery_error_ so we can tell in the
// end whether recovery succeeded or not
Status ErrorHandler::SetBGError(const Status& bg_err, BackgroundErrorReason reason) {
  db_mutex_->AssertHeld();
  if (bg_err.ok()) {
    return Status::OK();
  }

  bool paranoid = db_options_.paranoid_checks;
  Status::Severity sev = Status::Severity::kFatalError;
  Status new_bg_err;
  bool found = false;

  {
    auto entry = ErrorSeverityMap.find(std::make_tuple(reason, bg_err.code(),
          bg_err.subcode(), paranoid));
    if (entry != ErrorSeverityMap.end()) {
      sev = entry->second;
      found = true;
    }
  }

  if (!found) {
    auto entry = DefaultErrorSeverityMap.find(std::make_tuple(reason,
          bg_err.code(), paranoid));
    if (entry != DefaultErrorSeverityMap.end()) {
      sev = entry->second;
      found = true;
    }
  }

  if (!found) {
    auto entry = DefaultReasonMap.find(std::make_tuple(reason, paranoid));
    if (entry != DefaultReasonMap.end()) {
      sev = entry->second;
    }
  }

  new_bg_err = Status(bg_err, sev);

  // Check if recovery is currently in progress. If it is, we will save this
  // error so we can check it at the end to see if recovery succeeded or not
  if (recovery_in_prog_ && recovery_error_.ok()) {
    recovery_error_ = new_bg_err;
  }

  bool auto_recovery = auto_recovery_;
  if (new_bg_err.severity() >= Status::Severity::kFatalError && auto_recovery) {
    auto_recovery = false;
  }

  // Allow some error specific overrides
  if (new_bg_err == Status::NoSpace()) {
    new_bg_err = OverrideNoSpaceError(new_bg_err, &auto_recovery);
  }

  if (!new_bg_err.ok()) {
    Status s = new_bg_err;
    EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason, &s,
                                          db_mutex_, &auto_recovery);
    if (!s.ok() && (s.severity() > bg_error_.severity())) {
      bg_error_ = s;
    } else {
      // This error is less severe than previously encountered error. Don't
      // take any further action
      return bg_error_;
    }
  }

  if (auto_recovery) {
    recovery_in_prog_ = true;

    // Kick-off error specific recovery
    if (bg_error_ == Status::NoSpace()) {
      RecoverFromNoSpace();
    }
  }
  return bg_error_;
}

Status ErrorHandler::SetBGError(const IOStatus& bg_io_err,
                                BackgroundErrorReason reason) {
  db_mutex_->AssertHeld();
  if (bg_io_err.ok()) {
    return Status::OK();
  }
  ROCKS_LOG_WARN(db_options_.info_log, "Background IO error %s",
                 bg_io_err.ToString().c_str());

  if (recovery_in_prog_ && recovery_io_error_.ok()) {
    recovery_io_error_ = bg_io_err;
  }
  if (BackgroundErrorReason::kManifestWrite == reason) {
    // Always returns ok
    db_->DisableFileDeletionsWithLock();
  }
  Status new_bg_io_err = bg_io_err;
  Status s;
  if (bg_io_err.GetDataLoss()) {
    // FIrst, data loss is treated as unrecoverable error. So it can directly
    // overwrite any existing bg_error_.
    bool auto_recovery = false;
    Status bg_err(new_bg_io_err, Status::Severity::kUnrecoverableError);
    bg_error_ = bg_err;
    if (recovery_in_prog_ && recovery_error_.ok()) {
      recovery_error_ = bg_err;
    }
    EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason, &s,
                                          db_mutex_, &auto_recovery);
    return bg_error_;
  } else if (bg_io_err.GetRetryable()) {
    // Second, check if the error is a retryable IO error or not. if it is
    // retryable error and its severity is higher than bg_error_, overwrite
    // the bg_error_ with new error.
    // In current stage, for retryable IO error of compaction, treat it as
    // soft error. In other cases, treat the retryable IO error as hard
    // error.
    bool auto_recovery = false;
    EventHelpers::NotifyOnBackgroundError(db_options_.listeners, reason, &s,
                                          db_mutex_, &auto_recovery);
    if (BackgroundErrorReason::kCompaction == reason) {
      Status bg_err(new_bg_io_err, Status::Severity::kSoftError);
      if (bg_err.severity() > bg_error_.severity()) {
        bg_error_ = bg_err;
      }
      return bg_error_;
    } else {
      Status bg_err(new_bg_io_err, Status::Severity::kHardError);
      if (recovery_in_prog_ && recovery_error_.ok()) {
        recovery_error_ = bg_err;
      }
      if (bg_err.severity() > bg_error_.severity()) {
        bg_error_ = bg_err;
      }
      return StartRecoverFromRetryableBGIOError(bg_io_err);
    }
  } else {
    s = SetBGError(new_bg_io_err, reason);
  }
  return s;
}

Status ErrorHandler::OverrideNoSpaceError(Status bg_error,
                                          bool* auto_recovery) {
#ifndef ROCKSDB_LITE
  if (bg_error.severity() >= Status::Severity::kFatalError) {
    return bg_error;
  }

  if (db_options_.sst_file_manager.get() == nullptr) {
    // We rely on SFM to poll for enough disk space and recover
    *auto_recovery = false;
    return bg_error;
  }

  if (db_options_.allow_2pc &&
      (bg_error.severity() <= Status::Severity::kSoftError)) {
    // Don't know how to recover, as the contents of the current WAL file may
    // be inconsistent, and it may be needed for 2PC. If 2PC is not enabled,
    // we can just flush the memtable and discard the log
    *auto_recovery = false;
    return Status(bg_error, Status::Severity::kFatalError);
  }

  {
    uint64_t free_space;
    if (db_options_.env->GetFreeSpace(db_options_.db_paths[0].path,
                                      &free_space) == Status::NotSupported()) {
      *auto_recovery = false;
    }
  }

  return bg_error;
#else
  (void)auto_recovery;
  return Status(bg_error, Status::Severity::kFatalError);
#endif
}

void ErrorHandler::RecoverFromNoSpace() {
#ifndef ROCKSDB_LITE
  SstFileManagerImpl* sfm =
      reinterpret_cast<SstFileManagerImpl*>(db_options_.sst_file_manager.get());

  // Inform SFM of the error, so it can kick-off the recovery
  if (sfm) {
    sfm->StartErrorRecovery(this, bg_error_);
  }
#endif
}

Status ErrorHandler::ClearBGError() {
#ifndef ROCKSDB_LITE
  db_mutex_->AssertHeld();

  // Signal that recovery succeeded
  if (recovery_error_.ok()) {
    Status old_bg_error = bg_error_;
    bg_error_ = Status::OK();
    recovery_in_prog_ = false;
    EventHelpers::NotifyOnErrorRecoveryCompleted(db_options_.listeners,
                                                 old_bg_error, db_mutex_);
  }
  return recovery_error_;
#else
  return bg_error_;
#endif
}

Status ErrorHandler::RecoverFromBGError(bool is_manual) {
#ifndef ROCKSDB_LITE
  InstrumentedMutexLock l(db_mutex_);
  if (is_manual) {
    // If its a manual recovery and there's a background recovery in progress
    // return busy status
    if (recovery_in_prog_) {
      return Status::Busy();
    }
    recovery_in_prog_ = true;
  }

  if (bg_error_.severity() == Status::Severity::kSoftError) {
    // Simply clear the background error and return
    recovery_error_ = Status::OK();
    return ClearBGError();
  }

  // Reset recovery_error_. We will use this to record any errors that happen
  // during the recovery process. While recovering, the only operations that
  // can generate background errors should be the flush operations
  recovery_error_ = Status::OK();
  Status s = db_->ResumeImpl();
  // For manual recover, shutdown, and fatal error  cases, set
  // recovery_in_prog_ to false. For automatic background recovery, leave it
  // as is regardless of success or failure as it will be retried
  if (is_manual || s.IsShutdownInProgress() ||
      bg_error_.severity() >= Status::Severity::kFatalError) {
    recovery_in_prog_ = false;
  }
  return s;
#else
  (void)is_manual;
  return bg_error_;
#endif
}

Status ErrorHandler::StartRecoverFromRetryableBGIOError(IOStatus io_error) {
#ifndef ROCKSDB_LITE
  db_mutex_->AssertHeld();
  if (bg_error_.ok() || io_error.ok()) {
    return Status::OK();
  }
  if (db_options_.max_bgerror_resume_count <= 0 || recovery_in_prog_ ||
      recovery_thread_) {
    // Auto resume BG error is not enabled, directly return bg_error_.
    return bg_error_;
  }

  recovery_in_prog_ = true;
  recovery_thread_.reset(
      new port::Thread(&ErrorHandler::RecoverFromRetryableBGIOError, this));

  if (recovery_io_error_.ok() && recovery_error_.ok()) {
    return Status::OK();
  } else {
    TEST_SYNC_POINT("StartRecoverRetryableBGIOError:RecoverFail");
    return bg_error_;
  }
#else
  (void)io_error;
  return bg_error_;
#endif
}

// Automatic recover from Retryable BG IO error. Must be called after db
// mutex is released.
void ErrorHandler::RecoverFromRetryableBGIOError() {
#ifndef ROCKSDB_LITE
  TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeStart");
  TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeStart1");
  InstrumentedMutexLock l(db_mutex_);
  if (end_recovery_) {
    return;
  }
  int resume_count = db_options_.max_bgerror_resume_count;
  uint64_t wait_interval = db_options_.bgerror_resume_retry_interval;
  // Recover from the retryable error. Create a separate thread to do it.
  while (resume_count > 0) {
    if (end_recovery_) {
      return;
    }
    TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeResume0");
    TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeResume1");
    recovery_io_error_ = IOStatus::OK();
    recovery_error_ = Status::OK();
    Status s = db_->ResumeImpl();
    TEST_SYNC_POINT("RecoverFromRetryableBGIOError:AfterResume0");
    TEST_SYNC_POINT("RecoverFromRetryableBGIOError:AfterResume1");
    if (s.IsShutdownInProgress() ||
        bg_error_.severity() >= Status::Severity::kFatalError) {
      // If DB shutdown in progress or the error severity is higher than
      // Hard Error, stop auto resume and returns.
      TEST_SYNC_POINT("RecoverFromRetryableBGIOError:RecoverFail0");
      recovery_in_prog_ = false;
      return;
    }
    if (!recovery_io_error_.ok() &&
        recovery_error_.severity() <= Status::Severity::kHardError &&
        recovery_io_error_.GetRetryable()) {
      // If new BG IO error happens during auto recovery and it is retryable
      // and its severity is Hard Error or lower, the auto resmue sleep for
      // a period of time and redo auto resume if it is allowed.
      TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeWait0");
      TEST_SYNC_POINT("RecoverFromRetryableBGIOError:BeforeWait1");
      int64_t wait_until = db_->env_->NowMicros() + wait_interval;
      cv_.TimedWait(wait_until);
      TEST_SYNC_POINT("RecoverFromRetryableBGIOError:AfterWait0");
    } else {
      // There are three possibility: 1) recover_io_error is set during resume
      // and the error is not retryable, 2) recover is successful, 3) other
      // error happens during resume and cannot be resumed here.
      if (recovery_io_error_.ok() && recovery_error_.ok() && s.ok()) {
        // recover from the retryable IO error and no other BG errors. Clean
        // the bg_error and notify user.
        TEST_SYNC_POINT("RecoverFromRetryableBGIOError:RecoverSuccess");
        Status old_bg_error = bg_error_;
        bg_error_ = Status::OK();
        EventHelpers::NotifyOnErrorRecoveryCompleted(db_options_.listeners,
                                                     old_bg_error, db_mutex_);
        recovery_in_prog_ = false;
        return;
      } else {
        TEST_SYNC_POINT("RecoverFromRetryableBGIOError:RecoverFail1");
        // In this case: 1) recovery_io_error is more serious or not retryable
        // 2) other Non IO recovery_error happens. The auto recovery stops.
        recovery_in_prog_ = false;
        return;
      }
    }
    resume_count--;
  }
  recovery_in_prog_ = false;
  TEST_SYNC_POINT("RecoverFromRetryableBGIOError:LoopOut");
  return;
#else
  return;
#endif
}

void ErrorHandler::EndAutoRecovery() {
  db_mutex_->AssertHeld();
  if (!end_recovery_) {
    end_recovery_ = true;
  }
  cv_.SignalAll();
  db_mutex_->Unlock();
  if (recovery_thread_) {
    recovery_thread_->join();
  }
  db_mutex_->Lock();
  return;
}

}  // namespace ROCKSDB_NAMESPACE

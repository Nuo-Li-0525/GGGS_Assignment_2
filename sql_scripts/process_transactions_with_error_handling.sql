/* **********************************************************************
** Program Name: Process Transactions with Exception Handling          **
**               (CPRG 307 - Assignment 3 Part 2)                      **
** Author:       Nuo Li, Kurt Hablado, and Ehrl Balquin                **
** Created:      December 4, 2024                                      **
** Description:  Processes transactions from the NEW_TRANSACTIONS      **
**               table. Validates transaction data for errors, logs    **
**               invalid transactions, updates account balances, and   **
**               moves valid transactions to permanent tables.         **
********************************************************************** */

DECLARE
    -- Constants for transaction types
    k_transaction_type_debit CONSTANT CHAR(1) := 'D';
    k_transaction_type_credit CONSTANT CHAR(1) := 'C';

    -- Cursor for unique transactions
    CURSOR c_transaction IS
        SELECT DISTINCT transaction_no, transaction_date, description
        FROM new_transactions
        ORDER BY transaction_no;

    -- Cursor for transaction details
    CURSOR c_transaction_detail(p_transaction_no NUMBER) IS
        SELECT account_no, transaction_type, transaction_amount
        FROM new_transactions
        WHERE transaction_no = p_transaction_no;

    -- Global variable for error messages
    v_error_message VARCHAR2(200);

BEGIN
    -- Process each unique transaction
    FOR r_transaction IN c_transaction LOOP
        DECLARE
            -- Validation variables
            v_debit_total NUMBER(10, 2) := 0;
            v_credit_total NUMBER(10, 2) := 0;

            -- Transaction detail variables
            v_account_balance NUMBER(10, 2);
            v_default_transaction_type CHAR(1);

            -- Custom exceptions
            e_missing_transaction_no EXCEPTION;
            e_debit_credit_mismatch EXCEPTION;
            e_invalid_account_number EXCEPTION;
            e_invalid_transaction_amount EXCEPTION;
            e_invalid_transaction_type EXCEPTION;

        BEGIN
            -- Debug output for transaction processing
            DBMS_OUTPUT.PUT_LINE('Processing Transaction NO.: ' || r_transaction.transaction_no);

            -- Validate transaction number
            IF r_transaction.transaction_no IS NULL THEN
                RAISE e_missing_transaction_no;
            END IF;

            -- Validate debit and credit totals
            SELECT SUM(CASE WHEN transaction_type = k_transaction_type_debit THEN transaction_amount ELSE 0 END),
                   SUM(CASE WHEN transaction_type = k_transaction_type_credit THEN transaction_amount ELSE 0 END)
            INTO v_debit_total, v_credit_total
            FROM new_transactions
            WHERE transaction_no = r_transaction.transaction_no;

            IF v_debit_total <> v_credit_total THEN
                RAISE e_debit_credit_mismatch;
            END IF;

            -- Insert into TRANSACTION_HISTORY
            INSERT INTO transaction_history (transaction_no, transaction_date, description)
            VALUES (r_transaction.transaction_no, r_transaction.transaction_date, r_transaction.description);

            -- Process transaction details
            FOR r_transaction_detail IN c_transaction_detail(r_transaction.transaction_no) LOOP
                BEGIN
                    -- Validate account number
                    SELECT default_trans_type, account_balance
                    INTO v_default_transaction_type, v_account_balance
                    FROM account a
                    JOIN account_type at ON a.account_type_code = at.account_type_code
                    WHERE a.account_no = r_transaction_detail.account_no;

                    -- Validate transaction type
                    IF r_transaction_detail.transaction_type NOT IN (k_transaction_type_debit, k_transaction_type_credit) THEN
                        RAISE e_invalid_transaction_type;
                    END IF;

                    -- Validate transaction amount
                    IF r_transaction_detail.transaction_amount < 0 THEN
                        RAISE e_invalid_transaction_amount;
                    END IF;

                    -- Update account balance
                    IF v_default_transaction_type = k_transaction_type_debit THEN
                        IF r_transaction_detail.transaction_type = k_transaction_type_debit THEN
                            v_account_balance := v_account_balance + r_transaction_detail.transaction_amount;
                        ELSE
                            v_account_balance := v_account_balance - r_transaction_detail.transaction_amount;
                        END IF;
                    ELSE -- Credit default
                        IF r_transaction_detail.transaction_type = k_transaction_type_credit THEN
                            v_account_balance := v_account_balance + r_transaction_detail.transaction_amount;
                        ELSE
                            v_account_balance := v_account_balance - r_transaction_detail.transaction_amount;
                        END IF;
                    END IF;

                    -- Update account and insert into TRANSACTION_DETAIL
                    UPDATE account
                    SET account_balance = v_account_balance
                    WHERE account_no = r_transaction_detail.account_no;

                    INSERT INTO transaction_detail (account_no, transaction_no, transaction_type, transaction_amount)
                    VALUES (r_transaction_detail.account_no, r_transaction.transaction_no, r_transaction_detail.transaction_type, r_transaction_detail.transaction_amount);

                EXCEPTION
                    WHEN NO_DATA_FOUND THEN
                        v_error_message := 'Invalid account number for Account NO.: ' || r_transaction_detail.account_no;
                        RAISE e_invalid_account_number;
                END;
            END LOOP;

            -- Remove processed transaction
            DELETE FROM new_transactions WHERE transaction_no = r_transaction.transaction_no;

        EXCEPTION
            -- Handle specific errors
            WHEN e_missing_transaction_no THEN
                v_error_message := 'Transaction number is missing for NO.: ' || r_transaction.transaction_no;
            WHEN e_debit_credit_mismatch THEN
                v_error_message := 'Debit and credit totals do not match for NO.: ' || r_transaction.transaction_no;
            WHEN e_invalid_account_number THEN
                v_error_message := 'Invalid account number encountered.';
            WHEN e_invalid_transaction_amount THEN
                v_error_message := 'Transaction amount cannot be negative.';
            WHEN e_invalid_transaction_type THEN
                v_error_message := 'Transaction type must be D (debit) or C (credit).';
            WHEN OTHERS THEN
                v_error_message := 'Unexpected error: ' || SQLERRM;
        END;

        -- Log errors
        IF v_error_message IS NOT NULL THEN
            INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
            VALUES (r_transaction.transaction_no, r_transaction.transaction_date, r_transaction.description, v_error_message);
            v_error_message := NULL; -- Reset error message
        END IF;

    END LOOP;

    -- Commit changes
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        v_error_message := 'Fatal error occurred during processing: ' || SQLERRM;
        INSERT INTO wkis_error_log (transaction_no, transaction_date, description, error_msg)
        VALUES (NULL, SYSDATE, 'Fatal error', v_error_message);
        COMMIT; -- Persist error log entries
END;

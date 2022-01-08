/*
 * This file is part of Haveno.
 *
 * Haveno is free software: you can redistribute it and/or modify it
 * under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at
 * your option) any later version.
 *
 * Haveno is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
 * License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with Haveno. If not, see <http://www.gnu.org/licenses/>.
 */

package bisq.core.api;

import bisq.core.account.AccountState;

import bisq.common.config.Config;

import javax.inject.Inject;
import javax.inject.Singleton;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

import lombok.extern.slf4j.Slf4j;

@Singleton
@Slf4j
public class CoreAccountService {

    private final Config config;
    private AccountState accountState;

    // Temp in memory variable for testing.
    private boolean isOpen = false;

    @Inject
    public CoreAccountService(Config config) {
        this.config = config;
        this.accountState = new AccountState();
    }

    /**
     * Indicates if the Haveno account is created.
     * @return True if account exists.
     */
    public boolean accountExists() {
        return accountState.accountExists;
    }

    /**
     * Backup the account to a zip file. Throw error if !accountExists().
     * @return InputStream with the zip of the account.
     */
    public InputStream backupAccount() throws Exception {

        if (!accountExists()) {
            throw new IllegalStateException("Cannot backup non existing account");
        }

        // Pipe the serialized account object to stream which will be read by the gRPC client.
        try {
            PipedInputStream in = new PipedInputStream();
            ObjectOutputStream out = new ObjectOutputStream(new PipedOutputStream(in));
            new Thread(() -> {
                try {
                    out.writeObject(accountState);
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();

            return in;
        } catch (java.io.IOException ex) {
            throw new Exception("Error occurred while while backing up account", ex);
        }
    }

    /**
     * Change the Haveno account password. Throw error if !isAccountOpen().
     * @param password
     */
    public void changePassword(String password) {
        if (!isAccountOpen()) {
            throw new IllegalStateException("Cannot change password on unopened account");
        }

        accountState.password = password;
    }

    /**
     * Close the currently open account. Throw error if !isAccountOpen().
     * @throws Exception
     */
    public void closeAccount() {
        if (!isAccountOpen()) {
            throw new IllegalStateException("Cannot close unopened account");
        }
        this.isOpen = false;
    }

    /**
     * Create and open a new Haveno account. Throw error if accountExists().
     * @param password The password for the account.
     * @throws Exception
     */
    public void createAccount(String password) {
        if (accountExists()) {
            throw new IllegalStateException("Cannot create account if the account already exists");
        }

        accountState.password = password;
        accountState.accountExists = true;
    }

    /**
     * Permanently delete the Haveno account.
     */
    public void deleteAccount() {
        this.isOpen = false;
        accountState.accountExists = false;
        accountState.password = null;
    }

    /**
     * Open existing account. Throw error if `!accountExists()
     * @param password The password for the account.
     */
    public void openAccount(String password) {
        if (!accountExists()) {
            throw new IllegalStateException("Cannot open account if account does not exist");
        }
        if (password.equals(accountState.password)) {
            this.isOpen = true;
        }
        // todo: consider changing to boolean return to indicate the account was not opened.
    }

    /**
     * Indicates if the Haveno account is open and authenticated with the correct password.
     * @return True if account is open.
     */
    public boolean isAccountOpen() {
        return isOpen && accountState.accountExists && accountState.password != null;
    }

    public void restoreAccount(InputStream zipStream) throws Exception {
        if (accountExists()) {
            throw new IllegalStateException("Cannot restore account if there is an existing account");
        }

        ObjectInputStream in = new ObjectInputStream(zipStream);
        accountState = (AccountState) in.readObject();
    }


}

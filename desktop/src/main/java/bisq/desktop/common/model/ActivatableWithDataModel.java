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

package bisq.desktop.common.model;

public class ActivatableWithDataModel<D extends Activatable> extends WithDataModel<D> implements Activatable {

    public ActivatableWithDataModel(D dataModel) {
        super(dataModel);
    }

    @Override
    public final void _activate() {
        dataModel._activate();
        this.activate();
    }

    protected void activate() {
    }

    @Override
    public final void _deactivate() {
        dataModel._deactivate();
        this.deactivate();
    }

    protected void deactivate() {
    }
}

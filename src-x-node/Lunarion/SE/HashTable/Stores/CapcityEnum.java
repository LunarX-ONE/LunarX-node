package Lunarion.SE.HashTable.Stores;

public class CapcityEnum {
	public enum HashCapacity {
		Cap6M {
			public int getCapacity() {
				return 6291469;
			}

			public int getConflictLimitation() {
				return 20;
			}

			public int getLevel() {
				return 0;
			}
		},
		Cap25M {
			public int getCapacity() {
				return 25165843;
			}

			public int getConflictLimitation() {
				return 9;
			}

			public int getLevel() {
				return 1;
			}
		},
		Cap100M {
			public int getCapacity() {
				return 100663319;
			}

			public int getConflictLimitation() {
				return 12;
			}

			public int getLevel() {
				return 2;
			}
		},
		Cap400M {
			public int getCapacity() {
				return 402653189;
			}

			public int getConflictLimitation() {
				return 15;
			}

			public int getLevel() {
				return 3;
			}
		};
		public abstract int getCapacity();

		public abstract int getConflictLimitation();

		public abstract int getLevel();
	};

}

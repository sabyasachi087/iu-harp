/*
 * Copyright 2014 Indiana University
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.iu.harp.keyval;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntCountVal extends IntVal {

  private int count;

  public IntCountVal() {
  }

  public IntCountVal(int val, int c) {
    super(val);
    this.count = c;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(count);
  }

  @Override
  public void read(DataInput in) throws IOException {
    super.read(in);
    this.count = in.readInt();
  }

  @Override
  public int getSizeInBytes() {
    return super.getSizeInBytes() + 4;
  }
}

CREATE TABLE on_call_doctors (
                                 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                                 shift_id UUID NOT NULL,
                                 doctor_name VARCHAR(100) NOT NULL,
                                 is_on_call BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_on_call_doctors_shift
    ON on_call_doctors(shift_id);
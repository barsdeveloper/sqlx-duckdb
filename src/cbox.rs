use std::{
    mem::{self, ManuallyDrop},
    ops::{Deref, DerefMut},
};

#[derive(Debug)]
pub(crate) struct CBox<T> {
    ptr: T,
    dealloc: fn(T),
}

impl<T> CBox<T> {
    pub fn new(ptr: T, dealloc: fn(T)) -> Self {
        Self { ptr, dealloc }
    }

    pub fn as_ref(&self) -> CBox<&T> {
        CBox::new(&self.ptr, |v| {})
    }

    pub fn as_mut(&mut self) -> CBox<&mut T> {
        CBox::new(&mut self.ptr, |v| {})
    }

    pub fn into_raw(self) -> T {
        let mut this = ManuallyDrop::new(self);
        unsafe { std::ptr::read(&this.ptr) }
    }
}

impl<T> Drop for CBox<T> {
    fn drop(&mut self) {
        unsafe {
            (self.dealloc)(std::ptr::read(&self.ptr));
        }
    }
}

impl<T> Deref for CBox<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.ptr
    }
}

impl<T> DerefMut for CBox<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.ptr
    }
}

unsafe impl<T> Send for CBox<T> {}
